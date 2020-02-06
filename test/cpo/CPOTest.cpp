#include "CPOTest.h"
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/appbase/AppEssentials.h>

using namespace k2;
#define K2EXPECT(actual, exp) { \
    if (!((actual) == (exp))) { \
        K2ERROR((#actual) << " == " << (#exp)); \
        K2ERROR("+++++++++++++++++++++ Test FAIL ++++++++++++++++( actual=" << actual <<", exp="<< exp<< ")"); \
        throw std::runtime_error("test failed"); \
    } \
}

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
    ConfigVar<std::string> configEp("cpo_endpoint");
    _cpoEndpoint = RPC().getTXEndpoint(configEp());

    // let start() finish and then run the tests
    (void)seastar::sleep(1ms)
        .then([this] { return runTest1(); })
        .then([this] { return runTest2(); })
        .then([this] { return runTest3(); })
        .then([this] { return runTest4(); })
        .then([this] { return runTest5(); })
        .then([] {
            K2INFO("======= All tests passed ========");
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
    return seastar::make_ready_future<>();
}

seastar::future<> CPOTest::runTest1() {
    K2INFO(">>> Test1: get non-existent collection");
    auto request = dto::CollectionGetRequest{.name="collection1"};
    return RPC()
    .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, std::move(request), *_cpoEndpoint, 100ms)
    .then([](auto response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Status::S404_Not_Found());
    });
}

seastar::future<> CPOTest::runTest2() {
    K2INFO(">>> Test2: create a collection");
    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name="collection2",
            .hashScheme="hash-crc32c",
            .storageDriver="k23si",
            .capacity{
                .dataCapacityMegaBytes=1,
                .readIOPs=100,
                .writeIOPs=200
            }
        },
        .clusterEndpoints{}
    };
    return RPC()
    .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, std::move(request), *_cpoEndpoint, 1s)
    .then([](auto response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Status::S201_Created());
    });
}

seastar::future<> CPOTest::runTest3() {
    K2INFO(">>> Test3: create a collection over existing one");
    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name = "collection2",
            .hashScheme = "hash-crc32c",
            .storageDriver = "k23siBAD",
            .capacity{
                .dataCapacityMegaBytes = 1000,
                .readIOPs = 100000,
                .writeIOPs = 100000}
        },
        .clusterEndpoints{}
    };
    return RPC()
        .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, std::move(request), *_cpoEndpoint, 1s)
        .then([](auto response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Status::S403_Forbidden());
        });
}

seastar::future<> CPOTest::runTest4() {
    K2INFO(">>> Test4: read the collection we created in test2");
    auto request = dto::CollectionGetRequest{.name = "collection2"};
    return RPC()
        .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, std::move(request), *_cpoEndpoint, 100ms)
        .then([](auto response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Status::S200_OK());
            auto& md = resp.collection.metadata;
            K2EXPECT(md.name, "collection2");
            K2EXPECT(md.hashScheme, "hash-crc32c");
            K2EXPECT(md.storageDriver, "k23si");
            K2EXPECT(md.capacity.dataCapacityMegaBytes, 1);
            K2EXPECT(md.capacity.readIOPs, 100);
            K2EXPECT(md.capacity.writeIOPs, 200);
        });
}

seastar::future<> CPOTest::runTest5() {
    K2INFO(">>> Test5: create a collection with assignments");
    std::vector<String> eps{ "tcp+k2rpc://0.0.0.0:10000", "tcp+k2rpc://0.0.0.0:10001", "tcp+k2rpc://0.0.0.0:10002" };
    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name = "collectionAssign",
            .hashScheme = "hash-crc32c",
            .storageDriver = "k23si",
            .capacity{
                .dataCapacityMegaBytes = 1000,
                .readIOPs = 100000,
                .writeIOPs = 100000}},
        .clusterEndpoints = eps};
    return RPC()
        .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, std::move(request), *_cpoEndpoint, 1s)
        .then([](auto response) {
            // create the collection
            auto& [status, resp] = response;
            K2EXPECT(status, Status::S201_Created());
        })
        .then([] {
            // wait for collection to get assigned
            return seastar::sleep(100ms);
        })
        .then([this] {
            // check to make sure the collection is assigned
            auto request = dto::CollectionGetRequest{.name = "collectionAssign"};
            return RPC()
                .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, std::move(request), *_cpoEndpoint, 100ms);
        })
        .then([eps](auto response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Status::S200_OK());
            K2EXPECT(resp.collection.metadata.name, "collectionAssign");
            K2EXPECT(resp.collection.metadata.hashScheme, "hash-crc32c");
            K2EXPECT(resp.collection.metadata.storageDriver, "k23si");
            K2EXPECT(resp.collection.metadata.capacity.dataCapacityMegaBytes, 1000);
            K2EXPECT(resp.collection.metadata.capacity.readIOPs, 100000);
            K2EXPECT(resp.collection.metadata.capacity.writeIOPs, 100000);
            K2EXPECT(resp.collection.partitionMap.version, 3);
            K2EXPECT(resp.collection.partitionMap.partitions.size(), 3);

            // how many partitions we have
            uint64_t numparts = eps.size();
            auto max = std::numeric_limits<uint64_t>::max();
            // how big is each one
            uint64_t partSize = max / numparts;

            for (size_t i = 0; i < resp.collection.partitionMap.partitions.size(); ++i) {
                auto& p = resp.collection.partitionMap.partitions[i];
                K2EXPECT(p.rangeVersion, 1);
                K2EXPECT(p.astate, dto::AssignmentState::Assigned);
                K2EXPECT(p.assignmentVersion, 1);
                K2EXPECT(p.startKey, std::to_string(i * partSize));
                K2EXPECT(p.endKey, std::to_string(i == eps.size() -1 ? max : (i + 1) * partSize - 1));
                K2EXPECT(p.endpoint, eps[i]);
            }
        });
}
