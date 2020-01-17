#include "CPOTest.h"
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/appbase/AppEssentials.h>
using namespace k2;
#define K2REQUIRE(cond, msg) { if (!(cond)) {K2ERROR("+++++++++++++++++++++ Test FAIL ++++++++++++++++"); throw std::runtime_error(msg);}}
CPOTest::CPOTest():exitcode(0) {
    K2INFO("ctor");
}

CPOTest::~CPOTest() {
    K2INFO("dtor");
}

seastar::future<> CPOTest::stop() {
    K2INFO("stop");
    return seastar::make_ready_future<>();
}

seastar::future<> CPOTest::start() {
    // let start() finish and then run the tests
    (void)seastar::sleep(1ms)
    .then([this]{ return runTest1();})
    .then([this]{ return runTest2();})
    .then([this]{ return runTest3();})
    .then([this]{ return runTest4();})
    .then([]{
        K2INFO("======= All tests passed ========");
    })
    .handle_exception([this](auto exc) {
        try {
            std::rethrow_exception(exc);
        }
        catch(RPCDispatcher::RequestTimeoutException& exc) {
            K2ERROR("======= Test failed due to timeout ========");
            exitcode = -1;
        }
        catch(std::exception& e) {
        K2ERROR("======= Test failed with exception [" << e.what() <<"] ========");
        exitcode = -1;
        }
    })
    .finally([this] {
        K2INFO("======= Test ended ========");
        seastar::engine().exit(exitcode);
    });
    return seastar::make_ready_future<>();
}

seastar::future<> CPOTest::runTest1() {
    K2INFO(">>> Test1: get non-existent collection");
    auto request = dto::CollectionGetRequest{.name="collection1"};
    auto ep = RPC().getTXEndpoint(Config()["cpo_endpoint"].as<std::string>());
    return RPC()
    .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, std::move(request), *ep, 100ms)
    .then([](auto response) {
        auto [status, resp] = response;
        K2INFO("Received get response with status: " << status);
        K2REQUIRE(status == Status::S404_Not_Found(), "Expected code 'Not found'");
    });
}

seastar::future<> CPOTest::runTest2() {
    K2INFO(">>> Test2: create a collection");
    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name="collection2",
            .hashScheme="range",
            .storageDriver="k2-3si",
            .capacity{
                .dataCapacityMegaBytes=1,
                .readIOPs=100,
                .writeIOPs=200
            }
        }
    };
    auto ep = RPC().getTXEndpoint(Config()["cpo_endpoint"].as<std::string>());
    return RPC()
    .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, std::move(request), *ep, 1s)
    .then([](auto response) {
        auto [status, resp] = response;
        K2INFO("Received create response with status: " << status);
        K2REQUIRE(status == Status::S201_Created(), "Expected code '201 created'");
    });
}

seastar::future<> CPOTest::runTest3() {
    K2INFO(">>> Test3: create a collection over existing one");
    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name = "collection2",
            .hashScheme = "rangeBAD",
            .storageDriver = "k2-3siBAD",
            .capacity{
                .dataCapacityMegaBytes = 1000,
                .readIOPs = 100000,
                .writeIOPs = 100000}}};
    auto ep = RPC().getTXEndpoint(Config()["cpo_endpoint"].as<std::string>());
    return RPC()
        .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, std::move(request), *ep, 1s)
        .then([](auto response) {
            auto [status, resp] = response;
            K2INFO("Received create response with status: " << status);
            K2REQUIRE(status == Status::S403_Forbidden(), "Expected code '403 forbidden'");
        });
}

seastar::future<> CPOTest::runTest4() {
    K2INFO(">>> Test4: read the collection we created in test2");
    auto request = dto::CollectionGetRequest{.name = "collection2"};
    auto ep = RPC().getTXEndpoint(Config()["cpo_endpoint"].as<std::string>());
    return RPC()
        .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, std::move(request), *ep, 100ms)
        .then([](auto response) {
            auto [status, resp] = response;
            K2INFO("Received get response with status: " << status);
            K2REQUIRE(status == Status::S200_OK(), "Expected code '200 ok'");

            K2REQUIRE(resp.collection.metadata.name == "collection2", "");
            K2REQUIRE(resp.collection.metadata.hashScheme == "range", "");
            K2REQUIRE(resp.collection.metadata.storageDriver == "k2-3si", "");
            K2REQUIRE(resp.collection.metadata.capacity.dataCapacityMegaBytes == 1, "");
            K2REQUIRE(resp.collection.metadata.capacity.readIOPs == 100, "");
            K2REQUIRE(resp.collection.metadata.capacity.writeIOPs == 200, "");
        });
}
