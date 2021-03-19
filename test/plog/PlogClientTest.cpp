/*
MIT License

Copyright(c) 2020 Futurewei Cloud

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
#include <k2/persistence/plog_client/PlogClient.h>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/dto/PersistenceCluster.h>
#include <k2/dto/MessageVerbs.h>
using namespace k2;

namespace k2::log {
inline thread_local k2::logging::Logger ptest("k2::ptest");
}

class PlogTest {

private:
    int exitcode = -1;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    k2::PlogClient _client;
    k2::ConfigVar<std::vector<k2::String>> _plogConfigEps{"plog_server_endpoints"};
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;
    
    k2::String _plogId;

public:  // application lifespan
    PlogTest() {
        K2LOG_I(log::ptest, "ctor");
    }
    ~PlogTest() {
        K2LOG_I(log::ptest, "ctor");
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::ptest, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start() {
        K2LOG_I(log::ptest, "start");
        ConfigVar<String> configEp("cpo_url");
        _cpoEndpoint = RPC().getTXEndpoint(configEp());

        // let start() finish and then run the tests
        _testTimer.set_callback([this] {
            _testFuture = runTest2()
            .then([this] { return runTest3(); })
            .then([this] {
                K2LOG_I(log::ptest, "======= All tests passed ========");
                exitcode = 0;
            })
            .handle_exception([this](auto exc) {
                try {
                    std::rethrow_exception(exc);
                } catch (RPCDispatcher::RequestTimeoutException& exc) {
                    K2LOG_E(log::ptest, "======= Test failed due to timeout ========");
                    exitcode = -1;
                } catch (std::exception& e) {
                    K2LOG_E(log::ptest, "======= Test failed with exception [{}] ========", e.what());
                    exitcode = -1;
                }
            })
            .finally([this] {
                K2LOG_I(log::ptest, "======= Test ended ========");
                seastar::engine().exit(exitcode);
            });
        });
        _testTimer.arm(0ms);
        return seastar::make_ready_future<>();
    }

    seastar::future<> runTest1() {
        K2LOG_I(log::ptest, ">>> Test1: get non-existent persistence cluster");
        auto request = dto::PersistenceClusterGetRequest{.name="Persistence_Cluster_1"};
        return RPC()
        .callRPC<dto::PersistenceClusterGetRequest, dto::PersistenceClusterGetResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_GET, request, *_cpoEndpoint, 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::ptest, status, Statuses::S404_Not_Found);
        });
    }

    seastar::future<> runTest2() {
        K2LOG_I(log::ptest, ">>> Test2: create a persistence cluster");
        dto::PersistenceGroup group1{.name="Group1", .plogServerEndpoints = _plogConfigEps()};
        dto::PersistenceCluster cluster1;
        cluster1.name="Persistence_Cluster_1";
        cluster1.persistenceGroupVector.push_back(group1);

        auto request = dto::PersistenceClusterCreateRequest{.cluster=std::move(cluster1)};
        return RPC()
        .callRPC<dto::PersistenceClusterCreateRequest, dto::PersistenceClusterCreateResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_CREATE, request, *_cpoEndpoint, 1s)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::ptest, status, Statuses::S201_Created);
        });
    }

    seastar::future<> runTest3() {
        ConfigVar<String> configEp("cpo_url");
        K2LOG_I(log::ptest, ">>> Test3: read the persistence group we created in test2");
        return _client.init("Persistence_Cluster_1", configEp())
        .then([this] () {
            K2LOG_I(log::ptest, "Test3.1: create a plog");
            return _client.create();
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.2: double-create an exist plog");
            auto& [status, resp] = response;
            K2EXPECT(log::ptest, status, Statuses::S201_Created);
            _plogId = resp;
            
            dto::PlogCreateRequest request{.plogId = _plogId};
            std::vector<String> plogServerEndpoints = _plogConfigEps();
            auto ep = RPC().getTXEndpoint(plogServerEndpoints[0]);
            
            return RPC().callRPC<dto::PlogCreateRequest, dto::PlogCreateResponse>(dto::Verbs::PERSISTENT_CREATE, request, *ep, 1s);
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.3: append a plog");
            auto& [status, resp] = response;
            K2EXPECT(log::ptest, status, Statuses::S409_Conflict);
            
            Payload payload([] { return Binary(4096); });
            payload.write("1234567890");
            return _client.append(_plogId, 0, std::move(payload));
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.4: append a plog");
            auto& [status, offset] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            K2EXPECT(log::ptest, offset, 15);

            Payload payload([] { return Binary(4096); });
            payload.write("0987654321");
            return _client.append(_plogId, 15, std::move(payload));
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.5: append a plog");
            auto& [status, offset] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            K2EXPECT(log::ptest, offset, 30);

            Payload payload([] { return Binary(4096); });
            payload.write("2333333333");
            return _client.append(_plogId, 30, std::move(payload));
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.6: append a plog with wrong offset");
            auto& [status, offset] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            K2EXPECT(log::ptest, offset, 45);

            Payload payload([] { return Binary(4096); });
            payload.write("1234567890");
            return _client.append(_plogId, 100, std::move(payload));
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.7: read a plog");
            auto& [status, offset] = response;
            K2EXPECT(log::ptest, status, Statuses::S403_Forbidden);
            return _client.read(_plogId, 0, 15);
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.8: read a plog");
            auto& [status, payload] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            String str;
            payload.seek(0);
            payload.read(str);
            K2EXPECT(log::ptest, str, "1234567890");
            return _client.read(_plogId, 15, 15);
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.9: read a plog");
            auto& [status, payload] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            String str;
            payload.seek(0);
            payload.read(str);
            K2EXPECT(log::ptest, str, "0987654321");
            return _client.read(_plogId, 30, 15);
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.10: read multiple payloads");
            auto& [status, payload] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            String str;
            payload.seek(0);
            payload.read(str);
            K2EXPECT(log::ptest, str, "2333333333");
            return _client.read(_plogId, 0, 30);
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.11: seal a plog");
            auto& [status, payload] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            String str, str2;
            payload.seek(0);
            payload.read(str);
            K2EXPECT(log::ptest, str, "1234567890");
            payload.read(str2);
            K2EXPECT(log::ptest, str2, "0987654321");
            return _client.seal(_plogId, 45);
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.12: seal a sealed plog");
            auto& [status, offset] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            K2EXPECT(log::ptest, offset, 45);
            return _client.seal(_plogId, 15);
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.13: append a sealed plog");
            auto& [status, offset] = response;
            K2EXPECT(log::ptest, status, Statuses::S409_Conflict);
            K2EXPECT(log::ptest, offset, 45);
            
            Payload payload([] { return Binary(4096); });
            payload.write("1234567890");
            return _client.append(_plogId, 45, std::move(payload));
        })
        .then([this] (auto&& response){
            K2LOG_I(log::ptest, "Test3.14: get the information of a plog");
            auto& [status, offset] = response;
            K2EXPECT(log::ptest, status, Statuses::S409_Conflict);
            K2EXPECT(log::ptest, status.message, "plog is sealed");

            return _client.getPlogStatus(_plogId);
        })
        .then([this] (auto&& response){
            auto& [status, plogStatus] = response;
            K2EXPECT(log::ptest, status, Statuses::S200_OK);
            K2EXPECT(log::ptest, std::get<0>(plogStatus), 45);
            K2EXPECT(log::ptest, std::get<1>(plogStatus), true);

            return seastar::make_ready_future<>();
        });
    }
};

int main(int argc, char** argv) {
    k2::App app("PlogTest");
    app.addOptions()("cpo_url", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("plog_server_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the plog servers");
    app.addApplet<PlogTest>();
    return app.start(argc, argv);
}