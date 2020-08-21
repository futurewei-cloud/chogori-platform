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
#include <k2/dto/PartitionGroup.h>
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
        _testFuture = runTest1()
        .then([this] { return runTest2(); })
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
    K2INFO(">>> Test1: get non-existent partition map");
    auto request = dto::PartitionMapGetRequest{.offset=0};
    return RPC()
    .callRPC<dto::PartitionMapGetRequest, dto::PartitionMapGetResponse>(dto::Verbs::CPO_PERSISTENCE_GET, request, *_cpoEndpoint, 100ms)
    .then([](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S404_Not_Found);
    });
}

seastar::future<> PlogTest::runTest2() {
    K2INFO(">>> Test2: create a partition map");
    auto request = dto::PartitionGroupCreateRequest{.partitionName = "Group1", .plogServerEndpoints = _plogConfigEps()};
    return RPC()
    .callRPC<dto::PartitionGroupCreateRequest, dto::PartitionGroupCreateResponse>(dto::Verbs::CPO_PERSISTENCE_REGISTER, request, *_cpoEndpoint, 1s)
    .then([](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S201_Created);
    });
}

seastar::future<> PlogTest::runTest3() {
    K2INFO(">>> Test3: read the partition group we created in test2");
    auto request = dto::PartitionMapGetRequest{.offset=0};
    return RPC()
        .callRPC<dto::PartitionMapGetRequest, dto::PartitionMapGetResponse>(dto::Verbs::CPO_PERSISTENCE_GET, request, *_cpoEndpoint, 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S200_OK);
            auto& md = resp.partitionMap;
            auto& group = md["Group1"];
            K2INFO(group[0]<<" "<<group[1]<<" "<<group[2]);
        });
}