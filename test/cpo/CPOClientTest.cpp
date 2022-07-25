/*
MIT License

Copyright(c) 2022 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/Timestamp.h>
#include <k2/tso/client/Client.h>
#include <k2/cpo/CPOClient.h>
#include <unistd.h>


#include "Log.h"

const char* collname = "cpo_client_test_collection";

namespace k2::log {
inline thread_local k2::logging::Logger cpo_client_test("k2::cpo_client_test");
}

namespace k2 {
    using namespace cpo;
    class CPOClientTest {
    public:
        CPOClientTest() {
            K2LOG_I(log::cpo_client_test, "ctor");
        }
        ~CPOClientTest() {
            K2LOG_I(log::cpo_client_test, "dtor");
        }
        seastar::future<dto::Timestamp> getTimeNow() {
            return AppBase().getDist<tso::TSOClient>().local().getTimestamp();
        }
        seastar::future<> runTest1() {
            K2LOG_I(log::cpo_client_test, ">>> Test 1: Getting a non-existent collection");
            return _cpoClient.getPartitionGetterWithRetry(Deadline<>(100ms),String(collname))
            .then([this] (auto&& response) {
                auto&[status, resp] = response;
                K2EXPECT(log::ptest, status.is4xxNonRetryable(), true);
            });
        }
        seastar::future<> runTest2() {
            K2LOG_I(log::cpo_client_test, ">>> Test 2: Creating a collection");
            auto request = dto::CollectionCreateRequest{
                .metadata{
                    .name=collname,
                    .hashScheme=dto::HashScheme::HashCRC32C,
                    .storageDriver=dto::StorageDriver::K23SI,
                    .capacity{
                        .dataCapacityMegaBytes=1,
                        .readIOPs=100,
                        .writeIOPs=200,
                        .minNodes=1
                    },
                    .retentionPeriod = 1h*90*24
                },
                .rangeEnds{}
            };
            // fork and run the new nodepool. Partition checking should fail the collection create into retry

            return _cpoClient.createAndWaitForCollection(Deadline<>(100ms), std::move(request.metadata), std::move(request.rangeEnds))
            .then([this] (auto&& response) {
                auto&[status, resp] = response;
                K2EXPECT(log::cpo_client_test, status, Statuses::S201_Created);
            });
        }
        seastar::future<> runTest3() {
            K2LOG_I(log::cpo_client_test, ">>> Test 3: Read the collection created in test 2");
            return _cpoClient.getPartitionGetterWithRetry(Deadline<>(100ms),String(collname))
            .then([this] (auto&& response) {
                auto&[status, resp] = response;
                K2EXPECT(log::cpo_client_test, status, Statuses::S200_OK);
                auto& md = *resp.collection.metadata;
                K2EXPECT(log::cpo_client_test, md.name, "cpo_client_test_collection");
                K2EXPECT(log::cpo_client_test, md.hashSchema, dto::HashScheme::HashCRC32C);
                for (size_t i = 0; i < resp.collection.partitionMap.partitions.size(); ++i) {
                    auto& p = resp.collection.partitionMap.partitions[i];
                    K2EXPECT(log::cpo_client_test, p.keyRangeV.pvid.rangeVersion, 1);
                    K2EXPECT(log::cpo_client_test, p.astate, dto::AssignmentState::Assigned);
                    K2EXPECT(log::cpo_client_test, p.keyRangeV.pvid.assignmentVersion, 1);
                    K2EXPECT(log::cpo_client_test, p.keyRangeV.pvid.id, i);
                }
                K2EXPECT(log::cpo_client_test, md.storageDriver, dto::StorageDriver::K23SI);
                K2EXPECT(log::cpo_client_test, md.retentionPeriod, 1h*90*24);
                K2EXPECT(log::cpo_client_test, md.capacity.dataCapacityMegaBytes, 1);
                K2EXPECT(log::cpo_client_test, md.capacity.readIOPs, 100);
                K2EXPECT(log::cpo_client_test, md.capacity.writeIOPs, 200);
            });
        }
        seastar::future<> start() {
            K2LOG_I(log::cpo_client_test, "start");
            __cpo_client.init(_cpoConfigEp());
            _cpoEndpoint = RPC().getTXEndpoint(_cpoConfigEp());

            _testTimer.set_callback([this, configEp] {
                K2LOG_I(log::cpo_client_test, "testTimer");
                _testFuture = seastar::make_ready_future()
                .then([this] {
                    return getTimeNow();
                })
                .then([this](dto::Timestamp&&) {
                    return runTest1();
                })
                .then([this] {
                    K2LOG_I(log::cpo_client_test, "======= All tests passed ========");
                    exitcode = 0;
                })
                .handle_exception([this](auto exc) {
                    try {
                        std::rethrow_exception(exc);
                    } catch (RPCDispatcher::RequestTimeoutException& exc) {
                        K2LOG_E(log::cpo_client_test, "======= Test failed due to timeout ========");
                        exitcode = -1;
                    } catch (std::exception& e) {
                        K2LOG_E(log::cpo_client_test, "======= Test failed with exception [{}] ========", e.what());
                        exitcode = -1;
                    }
                })
                .finally([this] {
                    K2LOG_I(log::k23si, "======= Test ended ========");
                    AppBase().stop(exitcode);
                });
            });

            _testTimer.arm(0ms);
            return seastar::make_ready_future<>();
        }
    private:
        int exitcode = -1;
        seastar::future<> _testFuture = seastar::make_ready_future();
        seastar::timer<> _testTimer;
        cpo::CPOClient _cpoClient;
        ConfigVar<String> _cpoConfigEp{"cpo"};
        ConfigVar<String> _newNodepool{"new_nodepool"};
        std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    };
} // ns k2


int main(int argc, char** argv) {
    k2::App app("CPOClientTest");
    app.addOptions()("cpo", bpo::value<k2::String>(), "The endpoint of the CPO");
    app.addOptions()("new_nodepool", bpo::value<k2::String>(), "The command of starting the new nodepool");
    app.addApplet<k2::CPOClientTest>();
    app.addApplet<k2::tso::TSOClient>();

    return app.start(argc, argv);
}
