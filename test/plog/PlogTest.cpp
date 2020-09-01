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

#include "PlogTest.h"
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/dto/PersistenceGroup.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/appbase/AppEssentials.h>

using namespace k2;

PlogTest::PlogTest() {
    K2INFO("ctor");
}

PlogTest::~PlogTest() {
    K2INFO("dtor");
}

seastar::future<> PlogTest::gracefulStop() {
    K2INFO("stop");
    return std::move(_testFuture);
}

seastar::future<> PlogTest::start() {
    K2INFO("start");
    ConfigVar<String> configEp("cpo_url");
    _cpoEndpoint = RPC().getTXEndpoint(configEp());

    // let start() finish and then run the tests
    _testTimer.set_callback([this] {
        _testFuture = runTest2()
        .then([this] { return runTest3(); })
        .then([this] {
            K2INFO("======= All tests passed ========");
            exitcode = 0;
        })
        .handle_exception([this](auto exc) {
            try {
                std::rethrow_exception(exc);
            } catch (RPCDispatcher::RequestTimeoutException& exc) {
                K2ERROR("======= Test failed due to timeout ========");
                exitcode = -1;
            } catch (std::exception& e) {
                K2ERROR("======= Test failed with exception [" << e.what() << "] ========");
                exitcode = -1;
            }
        })
        .finally([this] {
            K2INFO("======= Test ended ========");
            seastar::engine().exit(exitcode);
        });
    });
    _testTimer.arm(0ms);
    return seastar::make_ready_future<>();
}

seastar::future<> PlogTest::runTest1() {
    K2INFO(">>> Test1: get non-existent persistence cluster");
    auto request = dto::PersistenceClusterGetRequest{.name="Cluster1"};
    return RPC()
    .callRPC<dto::PersistenceClusterGetRequest, dto::PersistenceClusterGetResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_GET, request, *_cpoEndpoint, 100ms)
    .then([](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S404_Not_Found);
    });
}

seastar::future<> PlogTest::runTest2() {
    K2INFO(">>> Test2: create a persistence cluster");
    dto::PersistenceGroup group1{.name="Group1", .plogServerEndpoints = _plogConfigEps()};
    dto::PersistenceCluster cluster1;
    cluster1.name="Cluster1";
    cluster1.persistenceGroupVector.push_back(group1);

    auto request = dto::PersistenceClusterCreateRequest{.cluster=std::move(cluster1)};
    return RPC()
    .callRPC<dto::PersistenceClusterCreateRequest, dto::PersistenceClusterCreateResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_CREATE, request, *_cpoEndpoint, 1s)
    .then([](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S201_Created);
    });
}

seastar::future<> PlogTest::runTest3() {
    K2INFO(">>> Test3: read the persistence group we created in test2");
    return _client.getPersistenceCluster("Cluster1")
    .then([this] () {
        K2INFO("Test3.1: create a plog");
        return _client.create();
    })
    .then([this] (auto&& response){
        K2INFO("Test3.2: append a plog");
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S201_Created);
        _plogId = resp;
        
        Payload payload([] { return Binary(4096); });
        payload.write("1234567890");
        return _client.append(_plogId, 0, std::move(payload));
    })
    .then([this] (auto&& response){
        K2INFO("Test3.3: append a plog");
        auto& [status, offset] = response;
        K2EXPECT(status, Statuses::S200_OK);
        K2EXPECT(offset, 15);

        Payload payload([] { return Binary(4096); });
        payload.write("0987654321");
        return _client.append(_plogId, 15, std::move(payload));
    })
    .then([this] (auto&& response){
        K2INFO("Test3.4: append a plog");
        auto& [status, offset] = response;
        K2EXPECT(status, Statuses::S200_OK);
        K2EXPECT(offset, 30);

        Payload payload([] { return Binary(4096); });
        payload.write("2333333333");
        return _client.append(_plogId, 30, std::move(payload));
    })
    .then([this] (auto&& response){
        K2INFO("Test3.5: append a plog with wrong offset");
        auto& [status, offset] = response;
        K2EXPECT(status, Statuses::S200_OK);
        K2EXPECT(offset, 45);

        Payload payload([] { return Binary(4096); });
        payload.write("1234567890");
        return _client.append(_plogId, 100, std::move(payload));
    })
    .then([this] (auto&& response){
        K2INFO("Test3.6: read a plog");
        auto& [status, offset] = response;
        K2EXPECT(status, Statuses::S400_Bad_Request);
        return _client.read(_plogId, 0, 15);
    })
    .then([this] (auto&& response){
        K2INFO("Test3.7: read a plog");
        auto& [status, payload] = response;
        K2EXPECT(status, Statuses::S200_OK);
        String str;
        payload.seek(0);
        payload.read(str);
        K2EXPECT(str, "1234567890");
        return _client.read(_plogId, 15, 15);
    })
    .then([this] (auto&& response){
        K2INFO("Test3.8: read a plog");
        auto& [status, payload] = response;
        K2EXPECT(status, Statuses::S200_OK);
        String str;
        payload.seek(0);
        payload.read(str);
        K2EXPECT(str, "0987654321");
        return _client.read(_plogId, 30, 15);
    })
    .then([this] (auto&& response){
        K2INFO("Test3.9: read multiple payloads");
        auto& [status, payload] = response;
        K2EXPECT(status, Statuses::S200_OK);
        String str;
        payload.seek(0);
        payload.read(str);
        K2EXPECT(str, "2333333333");
        return _client.read(_plogId, 0, 30);
    })
    .then([this] (auto&& response){
        K2INFO("Test3.10: seal a plog");
        auto& [status, payload] = response;
        K2EXPECT(status, Statuses::S200_OK);
        String str, str2;
        payload.seek(0);
        payload.read(str);
        K2EXPECT(str, "1234567890");
        payload.read(str2);
        K2EXPECT(str2, "0987654321");
        return _client.seal(_plogId, 45);
    })
    .then([this] (auto&& response){
        K2INFO("Test3.9: seal a sealed plog");
        auto& [status, offset] = response;
        K2EXPECT(status, Statuses::S200_OK);
        K2EXPECT(offset, 45);
        return _client.seal(_plogId, 15);
    })
    .then([this] (auto&& response){
        K2INFO("Test3.10: append a sealed plog");
        auto& [status, offset] = response;
        K2EXPECT(status, Statuses::S409_Conflict);
        K2EXPECT(offset, 45);
        
        Payload payload([] { return Binary(4096); });
        payload.write("1234567890");
        return _client.append(_plogId, 45, std::move(payload));
    })
    .then([this] (auto&& response){
        auto& [status, offset] = response;
        K2EXPECT(status, Statuses::S409_Conflict);
        K2EXPECT(status.message, "plog is sealed");
        return seastar::make_ready_future<>();
    });
}
