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
#include <k2/cpo/client/Client.h>
#include <k2/dto/Collection.h>
#include <k2/dto/ControlPlaneOracle.h>


const char* collname = "cpo_client_test_collection";

namespace k2::log {
inline thread_local k2::logging::Logger cpo_client_test("k2::cpo_client_test");
}

namespace k2 {
    using namespace cpo;
    class CPOClientTest {
    public:
        CPOClientTest() {
            K2LOG_I(k2::log::cpo_client_test, "ctor");
        }
        ~CPOClientTest() {
            K2LOG_I(k2::log::cpo_client_test, "dtor");
        }
        seastar::future<> gracefulStop() {
            K2LOG_I(k2::log::cpo_client_test, "stop");
            return std::move(_testFuture);
        }
        seastar::future<dto::Timestamp> getTimeNow() {
            return AppBase().getDist<tso::TSOClient>().local().getTimestamp();
        }
        seastar::future<> runTest1() {
            K2LOG_I(k2::log::cpo_client_test, ">>> Test 1: Getting a non-existent collection");
            return _cpoClient.getPartitionGetterWithRetry(Deadline<>(100ms),String(collname))
            .then([this] (auto&& response) {
                auto&[status, resp] = response;
                K2EXPECT(k2::log::cpo_client_test, status.is4xxNonRetryable(), true);
            });
        }
        seastar::future<> runTest2() {
            K2LOG_I(k2::log::cpo_client_test, ">>> Test 2: Creating a collection. Should fail because the last partition is not up and fails when checking all partition assigned");
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
            return _cpoClient.createAndWaitForCollection(Deadline<>(100ms), std::move(request.metadata), std::move(request.rangeEnds))
            .then([this] (auto&& status) {
                K2EXPECT(k2::log::cpo_client_test, status.is4xxNonRetryable(), true);
            });
        }
        seastar::future<> start() {
            K2LOG_I(k2::log::cpo_client_test, "start");
            _cpoClient.init(_cpoConfigEp());
            _cpoEndpoint = RPC().getTXEndpoint(_cpoConfigEp());
            _tsoClient = AppBase().getDist<tso::TSOClient>().local_shared();

            _testTimer.set_callback([this] {
                K2LOG_I(k2::log::cpo_client_test, "testTimer");
                _testFuture = seastar::make_ready_future()
                .then([this] {
                    return  _tsoClient->getTimestamp();
                })
                .then([this](dto::Timestamp&&) {
                    return runTest1();
                })
                .then([this] {
                    return runTest2();
                })
                .then([this] {
                    K2LOG_I(k2::log::cpo_client_test, "======= All tests passed ========");
                    exitcode = 0;
                })
                .handle_exception([this](auto exc) {
                    try {
                        std::rethrow_exception(exc);
                    } catch (RPCDispatcher::RequestTimeoutException& exc) {
                        K2LOG_E(k2::log::cpo_client_test, "======= Test failed due to timeout ========");
                        exitcode = -1;
                    } catch (std::exception& e) {
                        K2LOG_E(k2::log::cpo_client_test, "======= Test failed with exception [{}] ========", e.what());
                        exitcode = -1;
                    }
                })
                .finally([this] {
                    K2LOG_I(k2::log::cpo_client_test, "======= Test ended ========");
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
        seastar::shared_ptr<k2::tso::TSOClient> _tsoClient;
        ConfigVar<String> _cpoConfigEp{"cpo"};
        std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    };
} // ns k2


int main(int argc, char** argv) {
    k2::App app("CPOClientTest");
    app.addOptions()("cpo", bpo::value<k2::String>(), "The endpoint of the CPO");
    app.addApplet<k2::CPOClientTest>();
    app.addApplet<k2::tso::TSOClient>();

    return app.start(argc, argv);
}
