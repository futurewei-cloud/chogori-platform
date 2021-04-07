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

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <seastar/core/sleep.hh>
using namespace k2;
#include "Log.h"
const char* collname = "k23si_test_collection";

class SKVClientTest {

public:  // application lifespan
    SKVClientTest() : _client(K23SIClientConfig()) { K2LOG_I(log::k23si, "ctor");}
    ~SKVClientTest(){ K2LOG_I(log::k23si, "dtor");}

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2LOG_I(log::k23si, "start");

        _testTimer.set_callback([this] {
            _testFuture = seastar::make_ready_future()
            .then([this] () {
                return _client.start();
            })
            .then([this] {
                K2LOG_I(log::k23si, "Creating test collection...");
                return _client.makeCollection(collname);
            })
            .then([](auto&& status) {
                K2EXPECT(log::k23si, status.is2xxOK(), true);
            })
            .then([this] () {
                dto::Schema schema;
                schema.name = "schema";
                schema.version = 1;
                schema.fields = std::vector<dto::SchemaField> {
                        {dto::FieldType::STRING, "partition", false, false},
                        {dto::FieldType::STRING, "range", false, false},
                        {dto::FieldType::STRING, "f1", false, false},
                        {dto::FieldType::STRING, "f2", false, false},
                };

                schema.setPartitionKeyFieldsByName(std::vector<String>{"partition"});
                schema.setRangeKeyFieldsByName(std::vector<String> {"range"});

                return _client.createSchema(collname, std::move(schema));
            })
            .then([] (auto&& result) {
                K2EXPECT(log::k23si, result.status.is2xxOK(), true);
            })
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
                } catch (std::exception& e) {
                    K2LOG_E(log::k23si, "======= Test failed with exception [{}] ========", e.what());
                    exitcode = -1;
                } catch (...) {
                    K2LOG_E(log::k23si, "Test failed with unknown exception");
                    exitcode = -1;
                }
            })
            .finally([this] {
                K2LOG_I(log::k23si, "======= Test ended ========");
                seastar::engine().exit(exitcode);
            });
        });

        _testTimer.arm(0ms);
        return seastar::make_ready_future();
    }

private:
    int exitcode = -1;

    seastar::timer<> _testTimer;
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::future<> _writeFuture = seastar::make_ready_future();

    K23SIClient _client;
    uint64_t txnids = 10000;

public: // tests

// 1. Write a record and read it back via dynamic schema method
// 2 . Write a record twice in the same txn
seastar::future<> runScenario01() {
    K2LOG_I(log::k23si, "Scenario 01");
    K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey");
                    record.serializeNext<String>("rangekey");
                    record.serializeNext<String>("data1");
                    record.serializeNext<String>("data2");

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([this](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this] () {
                    return _client.getSchema(collname, "schema", K23SIClient::ANY_VERSION);
                })
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey");
                    record.serializeNext<String>("rangekey");

                    return txnHandle.read<dto::SKVRecord>(std::move(record));
                })
                .then([this](ReadResult<dto::SKVRecord>&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                    std::optional<String> partkey = response.value.deserializeNext<String>();
                    std::optional<String> rangekey = response.value.deserializeNext<String>();
                    std::optional<String> data1 = response.value.deserializeNext<String>();
                    std::optional<String> data2 = response.value.deserializeNext<String>();
                    K2EXPECT(log::k23si, *partkey, "partkey");
                    K2EXPECT(log::k23si, *rangekey, "rangekey");
                    K2EXPECT(log::k23si, *data1, "data1");
                    K2EXPECT(log::k23si, *data2, "data2");

                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                    return seastar::make_ready_future<>();
                });
        });
    })
    // Write the same key twice in same txn
    .then([this] () {
        K2TxnOptions options{};
        options.syncFinalize = true;
        return _client.beginTxn(options);
    })
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey");
                    record.serializeNext<String>("rangekey");
                    record.serializeNext<String>("data1-2");
                    record.serializeNext<String>("data2-2");

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this] () {
                    return _client.getSchema(collname, "schema", 1);
                })
                .then([&txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey");
                    record.serializeNext<String>("rangekey");
                    record.serializeNext<String>("data1-3");
                    record.serializeNext<String>("data2-3");

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([&txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                    return seastar::make_ready_future<>();
                });
        });
    });
}

// Try to issue an end request in parallel with a write
seastar::future<> runScenario02() {
    K2LOG_I(log::k23si, "Scenario 02");
    return _client.beginTxn(K2TxnOptions())
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey_02");
                    record.serializeNext<String>("rangekey_02");
                    record.serializeNext<String>("data1");
                    record.serializeNext<String>("data2");

                    _writeFuture = txnHandle.write<dto::SKVRecord>(record).discard_result();
                    return txnHandle.end(true);
                })
                .then([this](auto&& response) {
                    (void) response;
                    K2EXPECT(log::k23si, true, false); // We expect an exception
                    // Need to wait to avoid errors in shutdown
                    return std::move(_writeFuture);
                }).
                handle_exception([this] (auto exc) {
                    K2LOG_W_EXC(log::k23si, exc, "Got expected exception in scenario 02");
                    // Need to wait to avoid errors in shutdown
                    return std::move(_writeFuture);
                });
        });
    });
}

// Try to write after an end request
seastar::future<> runScenario03() {
    K2LOG_I(log::k23si, "Scenario 03");
    return _client.beginTxn(K2TxnOptions())
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey_s03");
                    record.serializeNext<String>("rangekey_s03");
                    record.serializeNext<String>("data1");
                    record.serializeNext<String>("data2");

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([this](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([this](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                    return _client.getSchema(collname, "schema", 1);
                })
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey_s03_2");
                    record.serializeNext<String>("rangekey_s03_2");
                    record.serializeNext<String>("data1");
                    record.serializeNext<String>("data2");

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([](auto&& response) {
                    (void) response;
                    K2EXPECT(log::k23si, false, true); // We expect an exception
                    return seastar::make_ready_future<>();
                })
                .handle_exception( [] (std::exception_ptr e) {
                    (void) e;
                    K2LOG_I(log::k23si, "Got expected exception with write after end request");
                    return seastar::make_ready_future<>();
                });
        });
    });
}

// scenario 04 partial update tests for the same schema version
seastar::future<> runScenario04() {
    K2LOG_I(log::k23si, "Scenario 04");
    K2TxnOptions options;
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    // records for read
                    dto::SKVRecord rdrecord3(collname, schemaPtr);
                    rdrecord3.serializeNext<String>("partkey_s04");
                    rdrecord3.serializeNext<String>("rangekey_s04");
                    dto::SKVRecord rdrecord4(collname, schemaPtr);
                    rdrecord4.serializeNext<String>("partkey_s04");
                    rdrecord4.serializeNext<String>("rangekey_s04");
                    dto::SKVRecord rdrecord5(collname, schemaPtr);
                    rdrecord5.serializeNext<String>("partkey_s04");
                    rdrecord5.serializeNext<String>("rangekey_s04");
                    dto::SKVRecord rdrecord6(collname, schemaPtr);
                    rdrecord6.serializeNext<String>("partkey_s04");
                    rdrecord6.serializeNext<String>("rangekey_s04");
                    dto::SKVRecord rdrecord7(collname, schemaPtr);
                    rdrecord7.serializeNext<String>("partkey_s04");
                    rdrecord7.serializeNext<String>("rangekey_s04");
                    dto::SKVRecord rdrecord8(collname, schemaPtr);
                    rdrecord8.serializeNext<String>("partkey_s04");
                    rdrecord8.serializeNext<String>("rangekey_s04");

                    // records for partial update
                    dto::SKVRecord record0(collname, schemaPtr);
                    dto::SKVRecord record1(collname, schemaPtr);
                    dto::SKVRecord record2(collname, schemaPtr);
                    dto::SKVRecord record3(collname, schemaPtr);
                    dto::SKVRecord record4(collname, schemaPtr);
                    dto::SKVRecord record5(collname, schemaPtr);
                    dto::SKVRecord record6(collname, schemaPtr);
                    dto::SKVRecord record7(collname, schemaPtr);
                    dto::SKVRecord record8(collname, schemaPtr);

                    // initialization
                    record0.serializeNext<String>("partkey_s04");
                    record0.serializeNext<String>("rangekey_s04");
                    record0.serializeNext<String>("data1_v1");
                    record0.serializeNext<String>("data2_v1");

                    // case1: Partial update value fields, with Skipped PartitionKey and RangeKey fields
                    record1.serializeNull();
                    record1.serializeNull();
                    record1.serializeNext<String>("data1_v2");
                    record1.serializeNext<String>("data2_v2");

                    // case2: Partial update including PartitionKey and RangeKey fields
                    record2.serializeNext<String>("partkey_s04_2");
                    record2.serializeNext<String>("rangekey_s04_2");
                    record2.serializeNext<String>("data3_v1");
                    record2.serializeNext<String>("data4_v1");

                    // case3: partial update every value fields
                    record3.serializeNext<String>("partkey_s04");
                    record3.serializeNext<String>("rangekey_s04");
                    record3.serializeNext<String>("data1_v2");
                    record3.serializeNext<String>("data2_v2");

                    // case4: Partial update some value fields(data2) using field name("f2") to indicate the fieldsForPartialUpdate
                    record4.serializeNext<String>("partkey_s04");
                    record4.serializeNext<String>("rangekey_s04");
                    record4.serializeNull();
                    record4.serializeNext<String>("data2_v3");

                    // case5: fieldsForPartialUpdate indicate some fields(data1&2) shall be updated, and it(data2) is skipped in the record
                    record5.serializeNext<String>("partkey_s04");
                    record5.serializeNext<String>("rangekey_s04");
                    record5.serializeNext<String>("data1_v3");
                    record5.serializeNull();

                    // case6: fieldsForPartialUpdate indicate some fields(data2) shall not be updated, but record has a value of this field
                    // fieldsForPartialUpdate shall prevail
                    record6.serializeNext<String>("partkey_s04");
                    record6.serializeNext<String>("rangekey_s04");
                    record6.serializeNull();
                    record6.serializeNext<String>("data2_v4");

                    // case7: Updates an null field while keeping other null fields null
                    record7.serializeNext<String>("partkey_s04");
                    record7.serializeNext<String>("rangekey_s04");
                    record7.serializeNext<String>("data1_v5");
                    record7.serializeNull();

                    return seastar::do_with(
                        std::move(record0),
                        std::move(record1),
                        std::move(record2),
                        std::move(record3),
                        std::move(record4),
                        std::move(record5),
                        std::move(record6),
                        std::move(record7),
                        std::move(rdrecord3),
                        std::move(rdrecord4),
                        std::move(rdrecord5),
                        std::move(rdrecord6),
                        std::move(rdrecord7),
                        [this, &txnHandle] (auto& rec0, auto& rec1, auto& rec2, auto& rec3, auto& rec4, auto& rec5, auto& rec6, auto& rec7,
                                auto& read3, auto& read4, auto& read5, auto& read6, auto& read7)  {
                        return txnHandle.write<dto::SKVRecord>(rec0)
                        .then([](auto&& response) {
                            K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                        })
                        .then([&] {
                            // case 1
                            return txnHandle.partialUpdate<dto::SKVRecord>(rec1, {0,1,2,3} )
                            .then([](auto&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::KeyNotFound); // because of the serialization added fields
                            });
                        })
                        .then([&] {
                            // case 2
                            return txnHandle.partialUpdate<dto::SKVRecord>(rec2, {2,3} )
                            .then([](auto&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::KeyNotFound);
                            });
                        })
                        .then([&] {
                            // case 3
                            return txnHandle.partialUpdate<dto::SKVRecord>(rec3, {2,3} )
                            .then([](auto&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<dto::SKVRecord>(std::move(read3))
                            .then([](ReadResult<dto::SKVRecord>&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                                std::optional<String> partkey = response.value.deserializeNext<String>();
                                std::optional<String> rangekey = response.value.deserializeNext<String>();
                                std::optional<String> data1 = response.value.deserializeNext<String>();
                                std::optional<String> data2 = response.value.deserializeNext<String>();
                                K2EXPECT(log::k23si, *partkey, "partkey_s04");
                                K2EXPECT(log::k23si, *rangekey, "rangekey_s04");
                                K2EXPECT(log::k23si, *data1, "data1_v2"); // partial update field
                                K2EXPECT(log::k23si, *data2, "data2_v2"); // partial update field

                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 4
                            return txnHandle.partialUpdate<dto::SKVRecord>(rec4, (std::vector<String>){"f2"})
                            .then([](auto&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<dto::SKVRecord>(std::move(read4))
                            .then([](ReadResult<dto::SKVRecord>&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                                std::optional<String> partkey = response.value.deserializeNext<String>();
                                std::optional<String> rangekey = response.value.deserializeNext<String>();
                                std::optional<String> data1 = response.value.deserializeNext<String>();
                                std::optional<String> data2 = response.value.deserializeNext<String>();
                                K2EXPECT(log::k23si, *partkey, "partkey_s04");
                                K2EXPECT(log::k23si, *rangekey, "rangekey_s04");
                                K2EXPECT(log::k23si, *data1, "data1_v2");
                                K2EXPECT(log::k23si, *data2, "data2_v3"); // partial update field

                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 5
                            return txnHandle.partialUpdate<dto::SKVRecord>(rec5, {2,3})
                            .then([](auto&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<dto::SKVRecord>(std::move(read5))
                            .then([](ReadResult<dto::SKVRecord>&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                                std::optional<String> partkey = response.value.deserializeNext<String>();
                                std::optional<String> rangekey = response.value.deserializeNext<String>();
                                std::optional<String> data1 = response.value.deserializeNext<String>();
                                std::optional<String> data2 = response.value.deserializeNext<String>();
                                K2EXPECT(log::k23si, *partkey, "partkey_s04");
                                K2EXPECT(log::k23si, *rangekey, "rangekey_s04");
                                K2EXPECT(log::k23si, *data1, "data1_v3");   // "f1" field was updated to v3
                                K2ASSERT(log::k23si, !data2, "data2 is nullOpt");         // "f2" field was updated to null

                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 6
                            return txnHandle.partialUpdate<dto::SKVRecord>(rec6, (std::vector<String>){"f1"})
                            .then([](auto&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<dto::SKVRecord>(std::move(read6))
                            .then([](ReadResult<dto::SKVRecord>&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                                std::optional<String> partkey = response.value.deserializeNext<String>();
                                std::optional<String> rangekey = response.value.deserializeNext<String>();
                                std::optional<String> data1 = response.value.deserializeNext<String>();
                                std::optional<String> data2 = response.value.deserializeNext<String>();
                                K2EXPECT(log::k23si, *partkey, "partkey_s04");
                                K2EXPECT(log::k23si, *rangekey, "rangekey_s04");
                                K2EXPECT(log::k23si, data1.has_value(), false);
                                K2EXPECT(log::k23si, data2.has_value(), false);

                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 7
                            return txnHandle.partialUpdate<dto::SKVRecord>(rec7, (std::vector<String>){"f1"})
                            .then([](auto&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<dto::SKVRecord>(std::move(read7))
                            .then([](ReadResult<dto::SKVRecord>&& response) {
                                K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                                std::optional<String> partkey = response.value.deserializeNext<String>();
                                std::optional<String> rangekey = response.value.deserializeNext<String>();
                                std::optional<String> data1 = response.value.deserializeNext<String>();
                                std::optional<String> data2 = response.value.deserializeNext<String>();
                                K2EXPECT(log::k23si, *partkey, "partkey_s04");
                                K2EXPECT(log::k23si, *rangekey, "rangekey_s04");
                                K2EXPECT(log::k23si, *data1, "data1_v5"); // update a null field
                                K2EXPECT(log::k23si, data2.has_value(), false);         // this field remains null

                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&txnHandle] () {
                            return txnHandle.end(true);
                        })
                        .then([] (auto&& response) {
                            K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                        });
                    }); // end do-with
                });
        }); // end do-with
    })
    // Previous test were all within same txn, so partial update was on top of a WI,
    // Now test partial update on top of a committed version
    .then([this, options] () {
        return _client.beginTxn(options);
    })
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([&txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey_s04");
                    record.serializeNext<String>("rangekey_s04");
                    record.serializeNull();
                    record.serializeNext<String>("data2_over_commit");

                    return txnHandle.partialUpdate<dto::SKVRecord>(record, std::vector<uint32_t>{3});
                })
                .then([] (auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([&txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([] (auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                    return seastar::make_ready_future<>();
                });
        });
    })
    .then([] {
        K2LOG_I(log::k23si, "scenario 04 partial update tests passed for the same schema version");
        return seastar::make_ready_future<>();
    });
}

// scenario 05 partial update tests for different schema versions
seastar::future<> runScenario05() {
    K2LOG_I(log::k23si, "Scenario 05");
    return seastar::make_ready_future<>()
    .then([this] {
        dto::Schema schema2, schema3;
        schema2.name = "schema";
        schema2.version = 2;
        schema2.fields = std::vector<dto::SchemaField> {
                {dto::FieldType::STRING, "partition", false, false},
                {dto::FieldType::STRING, "range", false, false},
                {dto::FieldType::STRING, "f2", false, false},
                {dto::FieldType::STRING, "f1", false, false},
        };
        schema2.setPartitionKeyFieldsByName(std::vector<String>{"partition"});
        schema2.setRangeKeyFieldsByName(std::vector<String> {"range"});

        schema3.name = "schema";
        schema3.version = 3;
        schema3.fields = std::vector<dto::SchemaField> {
                {dto::FieldType::STRING,  "partition", false, false},
                {dto::FieldType::STRING,  "range", false, false},
                {dto::FieldType::INT64T, "f3", false, false},
                {dto::FieldType::INT32T, "f2", false, false},
                {dto::FieldType::STRING,  "f1", false, false},
        };
        schema3.setPartitionKeyFieldsByName(std::vector<String>{"partition"});
        schema3.setRangeKeyFieldsByName(std::vector<String> {"range"});

        return seastar::when_all( _client.createSchema(collname, std::move(schema2)), _client.createSchema(collname, std::move(schema3)) );
    })
    .then([] (auto&& response) mutable {
        auto& [result1, result2] = response;
        auto status1 = result1.get0();
        auto status2 = result2.get0();
        K2EXPECT(log::k23si, status1.status.is2xxOK(), true);
        K2EXPECT(log::k23si, status2.status.is2xxOK(), true);
    })
    .then([this] {
        return _client.beginTxn(K2TxnOptions())
        .then([this] (K2TxnHandle&& txn) {
            return seastar::do_with(
                std::move(txn),
                [this](K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record0(collname, schemaPtr);
                    // initialization
                    record0.serializeNext<String>("partkey_s05");
                    record0.serializeNext<String>("rangekey_s05");
                    record0.serializeNext<String>("data1_v1");
                    record0.serializeNext<String>("data2_v1");
                    return txnHandle.write<dto::SKVRecord>(record0);
                })
                .then([this](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                // case 1: same fields but different orders
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record1(collname, schemaPtr);
                    record1.serializeNext<String>("partkey_s05");
                    record1.serializeNext<String>("rangekey_s05");
                    record1.serializeNext<String>("data2_v2");
                    record1.serializeNull();

                    return txnHandle.partialUpdate<dto::SKVRecord>(record1, std::vector<uint32_t>{2})
                    .then([](auto&& response) {
                        K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord read1(collname, schemaPtr);
                    read1.serializeNext<String>("partkey_s05");
                    read1.serializeNext<String>("rangekey_s05");

                    return txnHandle.read<dto::SKVRecord>(std::move(read1));
                })
                .then([this](ReadResult<dto::SKVRecord>&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                    std::optional<String> partkey = response.value.deserializeNext<String>();
                    std::optional<String> rangekey = response.value.deserializeNext<String>();
                    std::optional<String> data2 = response.value.deserializeNext<String>();
                    std::optional<String> data1 = response.value.deserializeNext<String>();

                    K2EXPECT(log::k23si, *partkey, "partkey_s05");
                    K2EXPECT(log::k23si, *rangekey, "rangekey_s05");
                    K2EXPECT(log::k23si, *data1, "data1_v1");
                    K2EXPECT(log::k23si, *data2, "data2_v2");
                })
                // case 2: missing a field with same field name but different field type
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record2(collname, schemaPtr);
                    record2.serializeNext<String>("partkey_s05");
                    record2.serializeNext<String>("rangekey_s05");
                    record2.serializeNext<int64_t>(64001234);
                    record2.serializeNull();
                    record2.serializeNext<String>("data1_v3");
                    return txnHandle.partialUpdate<dto::SKVRecord>(record2, {2,4})

                    .then([](auto&& response) {
                        K2EXPECT(log::k23si, response.status, dto::K23SIStatus::BadParameter);
                    });
                })
                // case 3: add some fields, but leaving them empty
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record3(collname, schemaPtr);
                    record3.serializeNext<String>("partkey_s05");
                    record3.serializeNext<String>("rangekey_s05");
                    record3.serializeNull();
                    record3.serializeNext<int32_t>(32001234);
                    record3.serializeNext<String>("data1_v3");
                    return txnHandle.partialUpdate<dto::SKVRecord>(record3, (std::vector<String>){"f1", "f2"})
                    .then([](auto&& response) {
                        K2EXPECT(log::k23si, response.status, dto::K23SIStatus::BadParameter);
                    });
                })
                // case 4: add some fields, do not contain some of the pre-existing fields
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record4(collname, schemaPtr);
                    record4.serializeNext<String>("partkey_s05");
                    record4.serializeNext<String>("rangekey_s05");
                    record4.serializeNext<int64_t>(64001234);
                    record4.serializeNext<int32_t>(32001234);
                    record4.serializeNull();
                    return txnHandle.partialUpdate<dto::SKVRecord>(record4, (std::vector<String>){"f3", "f2"})
                    .then([](auto&& response) {
                        K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord read4(collname, schemaPtr);
                    read4.serializeNext<String>("partkey_s05");
                    read4.serializeNext<String>("rangekey_s05");

                    return txnHandle.read<dto::SKVRecord>(std::move(read4));
                })
                .then([this](ReadResult<dto::SKVRecord>&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                    std::optional<String> partkey = response.value.deserializeNext<String>();
                    std::optional<String> rangekey = response.value.deserializeNext<String>();
                    std::optional<int64_t> data3 = response.value.deserializeNext<int64_t>();
                    std::optional<int32_t> data2 = response.value.deserializeNext<int32_t>();
                    std::optional<String> data1 = response.value.deserializeNext<String>();

                    K2EXPECT(log::k23si, *partkey, "partkey_s05");
                    K2EXPECT(log::k23si, *rangekey, "rangekey_s05");
                    K2EXPECT(log::k23si, *data3, 64001234); // partial update field
                    K2EXPECT(log::k23si, *data2, 32001234); // partial update field
                    K2EXPECT(log::k23si, *data1, "data1_v1");
                })
                // case 5: decrease some fields, and update value-fields to null-fields
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record5(collname, schemaPtr);
                    record5.serializeNext<String>("partkey_s05");
                    record5.serializeNext<String>("rangekey_s05");
                    record5.serializeNull();
                    record5.serializeNull();
                    return txnHandle.partialUpdate<dto::SKVRecord>(record5, (std::vector<String>){"f2", "f1"})
                    .then([](auto&& response) {
                        K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord read5(collname, schemaPtr);
                    read5.serializeNext<String>("partkey_s05");
                    read5.serializeNext<String>("rangekey_s05");

                    return txnHandle.read<dto::SKVRecord>(std::move(read5));
                })
                .then([this](ReadResult<dto::SKVRecord>&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                    std::optional<String> partkey = response.value.deserializeNext<String>();
                    std::optional<String> rangekey = response.value.deserializeNext<String>();
                    std::optional<String> data2 = response.value.deserializeNext<String>();
                    std::optional<String> data1 = response.value.deserializeNext<String>();

                    K2EXPECT(log::k23si, *partkey, "partkey_s05");
                    K2EXPECT(log::k23si, *rangekey, "rangekey_s05");
                    K2EXPECT(log::k23si, data2.has_value(), false); // parital update value-field to null
                    K2EXPECT(log::k23si, data1.has_value(), false); // parital update value-field to null
                })
                // case 6: update fields from null to null
                .then([&] {
                    return _client.getSchema(collname, "schema", 1);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record6(collname, schemaPtr);
                    record6.serializeNext<String>("partkey_s05");
                    record6.serializeNext<String>("rangekey_s05");
                    record6.serializeNull();
                    record6.serializeNull();
                    return txnHandle.partialUpdate<dto::SKVRecord>(record6, std::vector<uint32_t>{2,3})
                    .then([](auto&& response) {
                        K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 1);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord read6(collname, schemaPtr);
                    read6.serializeNext<String>("partkey_s05");
                    read6.serializeNext<String>("rangekey_s05");

                    return txnHandle.read<dto::SKVRecord>(std::move(read6));
                })
                .then([this](ReadResult<dto::SKVRecord>&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                    std::optional<String> partkey = response.value.deserializeNext<String>();
                    std::optional<String> rangekey = response.value.deserializeNext<String>();
                    std::optional<String> data1 = response.value.deserializeNext<String>();
                    std::optional<String> data2 = response.value.deserializeNext<String>();

                    K2EXPECT(log::k23si, *partkey, "partkey_s05");
                    K2EXPECT(log::k23si, *rangekey, "rangekey_s05");
                    K2EXPECT(log::k23si, data1.has_value(), false); // parital update null-field to null
                    K2EXPECT(log::k23si, data2.has_value(), false); // parital update null-field to null
                });
            }) // end do-with txnHandle
            .then([] {
                K2LOG_I(log::k23si, "scenario 05 partial update tests passed for different schema versions");
            });
        });
    });
}

seastar::future<> runScenario06() {
    K2LOG_I(log::k23si, "Scenario 06");
    return _client.beginTxn(K2TxnOptions())
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey");
                    // not specifying range key record.serializeNext<String>("rangekey");

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([this, &txnHandle] (auto&& result) {
                    (void) result;
                    K2EXPECT(log::k23si, false, true); // We expect exception
                    return seastar::make_ready_future<>();
                })
                .handle_exception( [] (std::exception_ptr e) {
                    (void) e;
                    K2LOG_I(log::k23si, "Got expected exception with unspecified rangeKey");
                    return seastar::make_ready_future<>();
                });
        });
    });
}

// Read and partial update using key-oriented interface
seastar::future<> runScenario07() {
    K2LOG_I(log::k23si, "Scenario 07");
    K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn), dto::Key(), std::shared_ptr<dto::Schema>(),
            [this] (K2TxnHandle& txnHandle, dto::Key& key, std::shared_ptr<dto::Schema>& my_schema) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle, &key, &my_schema] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey07");
                    record.serializeNext<String>("rangekey07");
                    record.serializeNext<String>("data1");
                    record.serializeNext<String>("data2");
                    key = record.getKey();
                    my_schema = schemaPtr;

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle, &key, &my_schema] () {
                    // Partial update without needing to serialize key
                    dto::SKVRecord record(collname, my_schema);
                    record.serializeNull();
                    record.serializeNull();
                    record.serializeNext<String>("partialupdate");
                    record.serializeNull();
                    std::vector<uint32_t> fields = {2};

                    return txnHandle.partialUpdate(record, fields, key);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([&txnHandle, &key, &my_schema] () {
                    // Read without needing to serialize key again
                    return txnHandle.read(key, collname);
                })
                .then([](ReadResult<dto::SKVRecord>&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);

                    std::optional<String> partkey = response.value.deserializeNext<String>();
                    std::optional<String> rangekey = response.value.deserializeNext<String>();
                    std::optional<String> data1 = response.value.deserializeNext<String>();
                    std::optional<String> data2 = response.value.deserializeNext<String>();

                    K2EXPECT(log::k23si, *partkey, "partkey07");
                    K2EXPECT(log::k23si, *rangekey, "rangekey07");
                    K2EXPECT(log::k23si, *data1, "partialupdate");
                    K2EXPECT(log::k23si, *data2, "data2");

                    return seastar::make_ready_future<>();
                })
                .then([&txnHandle] () {
                    return txnHandle.end(false);
                })
                .then([] (auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                    return seastar::make_ready_future<>();
                });
            }); // end do_with
        });
}

// Tests for rejectIfExists flag
seastar::future<> runScenario08() {
    K2LOG_I(log::k23si, "Scenario 08");
    K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn), std::shared_ptr<dto::Schema>(),
            [this] (K2TxnHandle& txnHandle, std::shared_ptr<dto::Schema>& my_schema) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle, &my_schema] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey08");
                    record.serializeNext<String>("rangekey08");
                    record.serializeNext<String>("data1");
                    record.serializeNext<String>("data2");
                    my_schema = schemaPtr;

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle, &my_schema] () {
                    dto::SKVRecord record(collname, my_schema);
                    record.serializeNext<String>("partkey08");
                    record.serializeNext<String>("rangekey08");
                    record.serializeNext<String>("data1*");
                    record.serializeNext<String>("data2*");

                    // rejectIfExists = true, we expect write to fail
                    return txnHandle.write(record, false, true);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::ConditionFailed);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle, &my_schema] () {
                    dto::SKVRecord record(collname, my_schema);
                    record.serializeNext<String>("partkey08");
                    record.serializeNext<String>("rangekey08");
                    record.serializeNull();
                    record.serializeNull();

                    // erase record
                    return txnHandle.write(record, true);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle, &my_schema] () {
                    dto::SKVRecord record(collname, my_schema);
                    record.serializeNext<String>("partkey08");
                    record.serializeNext<String>("rangekey08");
                    record.serializeNext<String>("data1*");
                    record.serializeNext<String>("data2*");

                    // rejectIfExists = true, we expect it to succeed after the erase
                    return txnHandle.write(record, false, true);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([&txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([] (auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                    return seastar::make_ready_future<>();
                });
            }); // end do_with
        });
}

// Read-your-erase
seastar::future<> runScenario09() {
    K2LOG_I(log::k23si, "Scenario 09");
    K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn), std::shared_ptr<dto::Schema>(),
            [this] (K2TxnHandle& txnHandle, std::shared_ptr<dto::Schema>& my_schema) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle, &my_schema] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(log::k23si, status.is2xxOK(), true);

                    dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<String>("partkey09");
                    record.serializeNext<String>("rangekey09");
                    record.serializeNext<String>("data1");
                    record.serializeNext<String>("data2");
                    my_schema = schemaPtr;

                    return txnHandle.write<dto::SKVRecord>(record);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle, &my_schema] () {
                    dto::SKVRecord record(collname, my_schema);
                    record.serializeNext<String>("partkey09");
                    record.serializeNext<String>("rangekey09");
                    record.serializeNull();
                    record.serializeNull();

                    return txnHandle.erase(record);
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle, &my_schema] () {
                    dto::SKVRecord record(collname, my_schema);
                    record.serializeNext<String>("partkey09");
                    record.serializeNext<String>("rangekey09");
                    record.serializeNull();
                    record.serializeNull();

                    return txnHandle.read<dto::SKVRecord>(std::move(record));
                })
                .then([](auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::KeyNotFound);
                    return seastar::make_ready_future<>();
                })
                .then([&txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([] (auto&& response) {
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
                    return seastar::make_ready_future<>();
                });
            }); // end do_with
        });
}

};  // class SKVClientTest

int main(int argc, char** argv) {
    App app("SKVClientTest");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<TSO_ClientLib>();
    app.addApplet<SKVClientTest>();
    return app.start(argc, argv);
}
