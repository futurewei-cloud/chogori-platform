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

#define CATCH_CONFIG_MAIN

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/module/k23si/Module.h>
#include <k2/cpo/client/CPOClient.h>
#include <seastar/core/sleep.hh>

#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>
#include <k2/dto/Collection.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include "Log.h"

namespace k2{
const char* collname = "skv_collection";
const char* scname = "skv_schema";
const char* scWoRKey = "skv_schema_wo_rkey";  // schema name without range key
const char* scETC = "skv_error_case";         // schema name: error test cases

class schemaCreation {
public: // application lifespan
    schemaCreation() { K2LOG_I(log::k23si, "ctor"); }
    ~schemaCreation() {K2LOG_I(log::k23si, "dtor"); }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start() {
    K2LOG_I(log::k23si, "+++++++ start schema creation test +++++++");
    ConfigVar<String> configEp("cpo_endpoint");
    _cpoEndpoint = RPC().getTXEndpoint(configEp());

    // let start() finish and then run the tests
    _testTimer.set_callback([this] {
        _testFuture = runScenario00()
        .then([this] { return runScenario01(); })
        .then([this] { return runScenario02(); })
        .then([this] { return runScenario03(); })
        .then([this] { return runScenario04(); })
        .then([this] { return runScenario05(); })
        .then([this] { return runScenario06(); })
        .then([this] { return runScenario07(); })
        .then([this] { return runScenario08(); })
        .then([this] { return runScenario09(); })
        .then([this] {
            K2LOG_I(log::k23si, "======= All tests passed ========");
            exitcode = 0;
        })
        .handle_exception([this](auto exc) {
            try {
                std::rethrow_exception(exc);
            } catch (RPCDispatcher::RequestTimeoutException& exc) {
                K2LOG_E(log::k23si, "======= Test failed due to timeout ========");
                exitcode = -1;
            } catch (std::exception& e) {
                K2LOG_E(log::k23si, "======= Test failed with exception [{}] ========", e.what());
                exitcode = -1;
            }
        })
        .finally([this] {
            K2LOG_I(log::k23si, "======= Test ended ========");
            seastar::engine().exit(exitcode);
        });
    });
    _testTimer.arm(0ms);
    return seastar::make_ready_future<>();
}


private:
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    k2::ConfigVar<std::vector<k2::String>> _k2ConfigEps{"k2_endpoints"};
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;
    int exitcode = -1;

    seastar::future<> createCollection(){
        K2LOG_I(log::k23si, "create a collection with assignments");

        auto request = dto::CollectionCreateRequest{
            .metadata{
                .name = collname,
                .hashScheme=dto::HashScheme::HashCRC32C,
                .storageDriver=dto::StorageDriver::K23SI,
                .capacity{
                    .dataCapacityMegaBytes = 100,
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
                K2EXPECT(log::k23si, status, Statuses::S201_Created);
            })
            .then([] {
                // wait for collection to get assigned
                return seastar::sleep(200ms);
            })
            .then([this] {
                // check to make sure the collection is assigned
                auto request = dto::CollectionGetRequest{.name = collname};
                return RPC()
                    .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms);
            })
            .then([this](auto&& response) {
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, Statuses::S200_OK);
                K2EXPECT(log::k23si, resp.collection.metadata.name, collname);
                K2EXPECT(log::k23si, resp.collection.metadata.hashScheme, dto::HashScheme::HashCRC32C);
                K2EXPECT(log::k23si, resp.collection.metadata.storageDriver, dto::StorageDriver::K23SI);
                K2EXPECT(log::k23si, resp.collection.metadata.retentionPeriod, 5h);
                K2EXPECT(log::k23si, resp.collection.metadata.capacity.dataCapacityMegaBytes, 100);
                K2EXPECT(log::k23si, resp.collection.metadata.capacity.readIOPs, 100000);
                K2EXPECT(log::k23si, resp.collection.metadata.capacity.writeIOPs, 100000);
                K2EXPECT(log::k23si, resp.collection.partitionMap.version, 1);
                K2EXPECT(log::k23si, resp.collection.partitionMap.partitions.size(), 3);

                // how many partitions we have
                uint64_t numparts = _k2ConfigEps().size();
                auto max = std::numeric_limits<uint64_t>::max();
                // how big is each one
                uint64_t partSize = max / numparts;

                for (size_t i = 0; i < resp.collection.partitionMap.partitions.size(); ++i) {
                    auto& p = resp.collection.partitionMap.partitions[i];
                    K2EXPECT(log::k23si, p.keyRangeV.pvid.rangeVersion, 1);
                    K2EXPECT(log::k23si, p.astate, dto::AssignmentState::Assigned);
                    K2EXPECT(log::k23si, p.keyRangeV.pvid.assignmentVersion, 1);
                    K2EXPECT(log::k23si, p.keyRangeV.pvid.id, i);
                    K2EXPECT(log::k23si, p.keyRangeV.startKey, std::to_string(i * partSize));
                    K2EXPECT(log::k23si, p.keyRangeV.endKey, std::to_string(i == _k2ConfigEps().size() - 1 ? max : (i + 1) * partSize - 1));
                    K2EXPECT(log::k23si, *p.endpoints.begin(), _k2ConfigEps()[i]);
                }
            });
    }



public: // tests

seastar::future<> runScenario00() {
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 00: initiate a schema with partition&range key +++++++");
    K2LOG_I(log::k23si, "STEP1: assign a collection named {}", collname);

    // step 1
    return createCollection()
    // step 2
    .then([this] {
        K2LOG_I(log::k23si, "------- create collection success. -------");
        K2LOG_I(log::k23si, "STEP2: create a shema named \"skv_schema\" with 3 fields {{FirstName | LastName | Balance}}");
        dto::Schema schema;
        schema.name = scname;
        schema.version = 1;
        schema.fields = std::vector<dto::SchemaField> {
                {dto::FieldType::STRING, "FirstName", false, false},
                {dto::FieldType::STRING, "LastName", false, false},
                {dto::FieldType::INT32T, "Balance", false, false}
        };

        schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
        schema.setRangeKeyFieldsByName(std::vector<String> {"FirstName"});

        dto::CreateSchemaRequest request{ collname, std::move(schema) };
        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);
            K2EXPECT(log::k23si, resp.schemas.size(), 1);
            K2EXPECT(log::k23si, resp.schemas[0].name, scname);

            K2LOG_I(log::k23si, "------- create schema success. -------");
            return seastar::make_ready_future<>();
        });
    });
}

// happy path test cases
seastar::future<> runScenario01(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 01: Create a new version of an existing schema by renaming a (non-key) field +++++++");

    K2LOG_I(log::k23si, "STEP1: get an existing Schema");
    dto::GetSchemasRequest request { collname };
    return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s)
    .then([this](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);


        K2LOG_I(log::k23si, "STEP2: renaming a non-key field and then create");
        auto schema(resp.schemas[0]);
        schema.version = 22;
        schema.fields[2].name = "Age";
        K2EXPECT(log::k23si, schema.basicValidation(), Statuses::S200_OK);
        K2EXPECT(log::k23si, resp.schemas[0].canUpgradeTo(schema), Statuses::S200_OK);

        dto::CreateSchemaRequest request{ collname, std::move(schema) };
        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);

            K2LOG_I(log::k23si, "------- create a new version of schema success. -------");
            return seastar::make_ready_future<>();
        });
    });
}

seastar::future<> runScenario02(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 02: Create a new version of an existing schema by adding a new field +++++++");

    K2LOG_I(log::k23si, "STEP1: get an existing Schema");
    dto::GetSchemasRequest request { collname };
    return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s)
    .then([this](auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);


        K2LOG_I(log::k23si, "STEP2: using version1 to add a new field \"Age\" with \"INT32_T\" fieldType");
        auto schema(resp.schemas[0]);
        schema.version = 333;
        schema.fields.push_back( {dto::FieldType::INT32T, "Age", false, false} );
        K2EXPECT(log::k23si, schema.basicValidation(), Statuses::S200_OK);
        K2EXPECT(log::k23si, resp.schemas[0].canUpgradeTo(schema), Statuses::S200_OK);

        dto::CreateSchemaRequest request{ collname, std::move(schema) };
        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);
            return seastar::make_ready_future<>();
        });
    });
}

seastar::future<> runScenario03(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 03: Create a schema which does not have any range key fields set +++++++");

    dto::Schema schema;
    schema.name = scWoRKey;
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "School", false, false},
            {dto::FieldType::STRING, "Major", false, false},
            {dto::FieldType::STRING, "Grade", false, false},
            {dto::FieldType::INT32T, "Class", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<String>{"School"});
    // set partitionKeyFields by index 1
    schema.partitionKeyFields.push_back(1);

    dto::CreateSchemaRequest request{ collname, std::move(schema) };
    return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);

        dto::GetSchemasRequest request { collname };
        return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
    })
    .then([] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);

        K2LOG_I(log::k23si, "------- create schema success. -------");
        return seastar::make_ready_future<>();
    });
}

// Error test cases
seastar::future<> runScenario04(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 04: Create a schema with duplicate field names +++++++");
    dto::Schema schema;
    schema.name = scETC;
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "School", false, false},
            {dto::FieldType::STRING, "School", false, false},
            {dto::FieldType::STRING, "Grade", false, false},
            {dto::FieldType::INT32T, "Class", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<String>{"School"});

    dto::CreateSchemaRequest request{ collname, std::move(schema) };
    return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S400_Bad_Request);

        dto::GetSchemasRequest request { collname };
        return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
    })
    .then([] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);

        for(auto& schema : resp.schemas) {
            if (schema.name == scETC) {
                K2ASSERT(log::k23si, false, "Schema should not exist");
            }
        }
        return seastar::make_ready_future();
    });
}

seastar::future<> runScenario05(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 05: Create a schema by setting partitionKeyFields manually by index, and an index is out of bounds of the fields +++++++");
    dto::Schema schema;
    schema.name = scETC;
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "School", false, false},
            {dto::FieldType::STRING, "Grade", false, false},
            {dto::FieldType::INT32T, "Class", false, false}
    };
    // set partitionKeyFields manually by index, and an index is out of bounds of the fields
    schema.setPartitionKeyFieldsByName(std::vector<String>{"School"});
    schema.partitionKeyFields.push_back(3);

    dto::CreateSchemaRequest request{ collname, std::move(schema) };
    return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S400_Bad_Request);

        dto::GetSchemasRequest request { collname };
        return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
    })
    .then([] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);

        for(auto& schema : resp.schemas) {
            if (schema.name == scETC) {
                K2ASSERT(log::k23si, false, "Schema shoud not exist");
            }
        }
        return seastar::make_ready_future();

    });
}

seastar::future<> runScenario06(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 06: Create a schema where the field at index 0 is not a partition or range key field +++++++");
    dto::Schema schema;
    schema.name = scETC;
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "School", false, false},
            {dto::FieldType::STRING, "Major", false, false},
            {dto::FieldType::STRING, "Grade", false, false},
            {dto::FieldType::INT32T, "Class", false, false}
    };
    // set partitionKeyFields manually by index, and index 0 is not a partition or range key field
    schema.partitionKeyFields.push_back(1);
    schema.rangeKeyFields.push_back(2);

    dto::CreateSchemaRequest request{ collname, std::move(schema) };
    return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S400_Bad_Request);

        dto::GetSchemasRequest request { collname };
        return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
    })
    .then([] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);

        for(auto& schema : resp.schemas) {
            if (schema.name == scETC) {
                K2ASSERT(log::k23si, false, "Schema should not exist but was found");
            }
        }
        return seastar::make_ready_future<>();
    });
}

seastar::future<> runScenario07(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 07: Create a new version of an existing schema where a key field is renamed +++++++");
    dto::GetSchemasRequest request { collname };
    return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);
        dto::Schema schema(resp.schemas[0]);
        schema.version = 4444;
        schema.fields[0].name = "Xing";

        dto::CreateSchemaRequest request{ collname, std::move(schema) };
        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S409_Conflict);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);

            for(auto& schema : resp.schemas) {
                if (schema.fields[0].name == "Xing"){
                    K2ASSERT(log::k23si, false, "Schema found but should not exist");
                }
            }
            return seastar::make_ready_future();
        });
    });
}

seastar::future<> runScenario08(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 08: Create a new version of an existing schema where the type of a key field changes +++++++");

    dto::GetSchemasRequest request { collname };
    return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);
        dto::Schema *sc;
        for(auto& schema : resp.schemas){
            if (schema.name == scWoRKey){
                sc = &schema;
                schema.version = 22;
                // the type of a key field changes
                schema.fields[1].type = dto::FieldType::INT32T;
                break;
            }
        }
        dto::CreateSchemaRequest request{ collname, std::move(*sc) };
        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S409_Conflict);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);
            for(auto& schema : resp.schemas) {
                if (schema.name == scWoRKey && schema.fields[1].type == dto::FieldType::INT32T) {
                    K2ASSERT(log::k23si, false, "Schema found but should not exist");
                }
            }
            return seastar::make_ready_future();
        });
    });
}

seastar::future<> runScenario09(){
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 09: Create a new version of an existing schema where a key field is removed +++++++");

    dto::GetSchemasRequest request { collname };
    return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s)
    .then([this] (auto&& response) {
        auto& [status, resp] = response;
        K2EXPECT(log::k23si, status, Statuses::S200_OK);
        dto::Schema *sc;
        for(auto& schema : resp.schemas){
            if (schema.name == scWoRKey){
                sc = &schema;
                schema.version = 333;
                //  Remove a key field[0]
                std::vector<SchemaField>::iterator it = schema.fields.begin();
                schema.fields.erase(it);
                break;
            }
        }
        dto::CreateSchemaRequest request{ collname, std::move(*sc) };
        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S409_Conflict);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);
            for(auto& schema : resp.schemas) {
                if (schema.name == scWoRKey && schema.version == 333){
                    K2ASSERT(log::k23si, false, "Schema found but should not exist");
                }
            }
            return seastar::make_ready_future();
        });
    });
}


}; // class schemaCreation
} //  ns k2


int main(int argc, char** argv) {
    k2::App app("schemaCreationTest");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
    app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO");
    app.addApplet<k2::schemaCreation>();
    return app.start(argc, argv);
}

