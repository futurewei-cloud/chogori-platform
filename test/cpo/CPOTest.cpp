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

#include "CPOTest.h"
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/appbase/AppEssentials.h>

using namespace k2;

CPOTest::CPOTest() {
    K2INFO("ctor");
}

CPOTest::~CPOTest() {
    K2INFO("dtor");
}

seastar::future<> CPOTest::gracefulStop() {
    K2INFO("stop");
    return std::move(_testFuture);
}

seastar::future<> CPOTest::start() {
    K2INFO("start");
    ConfigVar<String> configEp("cpo_endpoint");
    _cpoEndpoint = RPC().getTXEndpoint(configEp());

    // let start() finish and then run the tests
    _testTimer.set_callback([this] {
        _testFuture = runTest1()
        .then([this] { return runTest2(); })
        .then([this] { return runTest3(); })
        .then([this] { return runTest4(); })
        .then([this] { return runTest5(); })
        .then([this] { return runTest6(); })
        .then([this] { return runTest7(); })
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

seastar::future<> CPOTest::runTest1() {
    K2INFO(">>> Test1: get non-existent collection");
    auto request = dto::CollectionGetRequest{.name="collection1"};
    return RPC()
    .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms)
    .then([](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S404_Not_Found);
    });
}

seastar::future<> CPOTest::runTest2() {
    K2INFO(">>> Test2: create a collection");
    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name="collection2",
            .hashScheme=dto::HashScheme::HashCRC32C,
            .storageDriver=dto::StorageDriver::K23SI,
            .capacity{
                .dataCapacityMegaBytes=1,
                .readIOPs=100,
                .writeIOPs=200
            },
            .retentionPeriod = 1h*90*24
        },
        .clusterEndpoints{},
        .rangeEnds{}
    };
    return RPC()
    .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s)
    .then([](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S201_Created);
    });
}

seastar::future<> CPOTest::runTest3() {
    K2INFO(">>> Test3: create a collection over existing one");
    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name = "collection2",
            .hashScheme=dto::HashScheme::HashCRC32C,
            .storageDriver=dto::StorageDriver::K23SI,
            .capacity{
                .dataCapacityMegaBytes = 1000,
                .readIOPs = 100000,
                .writeIOPs = 100000
            },
            .retentionPeriod = 1h
        },
        .clusterEndpoints{},
        .rangeEnds{}
    };

    return RPC()
        .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S403_Forbidden);
        });
}

seastar::future<> CPOTest::runTest4() {
    K2INFO(">>> Test4: read the collection we created in test2");
    auto request = dto::CollectionGetRequest{.name = "collection2"};
    return RPC()
        .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S200_OK);
            auto& md = resp.collection.metadata;
            K2EXPECT(md.name, "collection2");
            K2EXPECT(md.hashScheme, dto::HashScheme::HashCRC32C);
            K2EXPECT(md.storageDriver, dto::StorageDriver::K23SI);
            K2EXPECT(md.retentionPeriod, 1h*90*24);
            K2EXPECT(md.capacity.dataCapacityMegaBytes, 1);
            K2EXPECT(md.capacity.readIOPs, 100);
            K2EXPECT(md.capacity.writeIOPs, 200);
        });
}

seastar::future<> CPOTest::runTest5() {
    K2INFO(">>> Test5: create a collection with assignments");

    auto request = dto::CollectionCreateRequest{
        .metadata{
            .name = "collectionAssign",
            .hashScheme=dto::HashScheme::HashCRC32C,
            .storageDriver=dto::StorageDriver::K23SI,
            .capacity{
                .dataCapacityMegaBytes = 1000,
                .readIOPs = 100000,
                .writeIOPs = 100000
            },
            .retentionPeriod = 5h
        },
        .clusterEndpoints = _k2ConfigEps(),
        .rangeEnds{}
    };
    return RPC()
        .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s)
        .then([](auto&& response) {
            // create the collection
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S201_Created);
        })
        .then([] {
            // wait for collection to get assigned
            return seastar::sleep(100ms);
        })
        .then([this] {
            // check to make sure the collection is assigned
            auto request = dto::CollectionGetRequest{.name = "collectionAssign"};
            return RPC()
                .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms);
        })
        .then([this](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S200_OK);
            K2EXPECT(resp.collection.metadata.name, "collectionAssign");
            K2EXPECT(resp.collection.metadata.hashScheme, dto::HashScheme::HashCRC32C);
            K2EXPECT(resp.collection.metadata.storageDriver, dto::StorageDriver::K23SI);
            K2EXPECT(resp.collection.metadata.retentionPeriod, 5h);
            K2EXPECT(resp.collection.metadata.capacity.dataCapacityMegaBytes, 1000);
            K2EXPECT(resp.collection.metadata.capacity.readIOPs, 100000);
            K2EXPECT(resp.collection.metadata.capacity.writeIOPs, 100000);
            K2EXPECT(resp.collection.partitionMap.version, 1);
            K2EXPECT(resp.collection.partitionMap.partitions.size(), 3);

            // how many partitions we have
            uint64_t numparts = _k2ConfigEps().size();
            auto max = std::numeric_limits<uint64_t>::max();
            // how big is each one
            uint64_t partSize = max / numparts;

            for (size_t i = 0; i < resp.collection.partitionMap.partitions.size(); ++i) {
                auto& p = resp.collection.partitionMap.partitions[i];
                K2EXPECT(p.pvid.rangeVersion, 1);
                K2EXPECT(p.astate, dto::AssignmentState::Assigned);
                K2EXPECT(p.pvid.assignmentVersion, 1);
                K2EXPECT(p.pvid.id, i);
                K2EXPECT(p.startKey, std::to_string(i * partSize));
                K2EXPECT(p.endKey, std::to_string(i == _k2ConfigEps().size() - 1 ? max : (i + 1) * partSize - 1));
                K2EXPECT(*p.endpoints.begin(), _k2ConfigEps()[i]);
            }
        });
}

seastar::future<> CPOTest::runTest6() {
    K2INFO(">>> Test6: Add a schema and get it back");

    dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::DocumentFieldType::STRING, "LastName", false, false},
            {dto::DocumentFieldType::STRING, "FirstName", false, false},
            {dto::DocumentFieldType::UINT32T, "Balance", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    dto::CreateSchemaRequest request{ "collectionAssign", std::move(schema) };
    return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S200_OK);

        dto::GetSchemasRequest request { "collectionAssign" };
        return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
    })
    .then([] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status, Statuses::S200_OK);
        K2EXPECT(resp.schemas.size(), 1);
        K2EXPECT(resp.schemas[0].name, "test_schema");
        
        return seastar::make_ready_future<>();
    });
}

seastar::future<> CPOTest::runTest7() {
    K2INFO(">>> Test7: Try to add an invalid schema");

    dto::Schema schema;
    schema.name = "invalid_schema";
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::DocumentFieldType::STRING, "LastName", false, false},
            {dto::DocumentFieldType::STRING, "FirstName", false, false},
            {dto::DocumentFieldType::UINT32T, "Balance", false, false}
    };
    schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});
    // Invalid schema because we did not set any partition key fields


    dto::CreateSchemaRequest request{ "collectionAssign", std::move(schema) };
    return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(status.is2xxOK(), false);
    });
}
