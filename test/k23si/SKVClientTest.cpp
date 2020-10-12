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

const char* collname = "k23si_test_collection";

class SKVClientTest {

public:  // application lifespan
    SKVClientTest() : _client(k2::K23SIClientConfig()) { K2INFO("ctor");}
    ~SKVClientTest(){ K2INFO("dtor");}

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2INFO("start");

        _testTimer.set_callback([this] {
            _testFuture = seastar::make_ready_future()
            .then([this] () {
                return _client.start();
            })
            .then([this] {
                K2INFO("Creating test collection...");
                return _client.makeCollection(collname);
            })
            .then([](auto&& status) {
                K2EXPECT(status.is2xxOK(), true);
            })
            .then([this] () {
                k2::dto::Schema schema;
                schema.name = "schema";
                schema.version = 1;
                schema.fields = std::vector<k2::dto::SchemaField> {
                        {k2::dto::FieldType::STRING, "partition", false, false},
                        {k2::dto::FieldType::STRING, "range", false, false},
                        {k2::dto::FieldType::STRING, "f1", false, false},
                        {k2::dto::FieldType::STRING, "f2", false, false},
                };

                schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"partition"});
                schema.setRangeKeyFieldsByName(std::vector<k2::String> {"range"});

                return _client.createSchema(collname, std::move(schema));
            })
            .then([] (auto&& result) {
                K2EXPECT(result.status.is2xxOK(), true);
            })
            .then([this] { return runScenario01(); })
            .then([this] { return runScenario02(); })
            .then([this] { return runScenario03(); })
            .then([this] { return runScenario04(); })
            .then([this] { return runScenario05(); })
            .then([this] {
                K2INFO("======= All tests passed ========");
                exitcode = 0;
            })
            .handle_exception([this](auto exc) {
                try {
                    std::rethrow_exception(exc);
                } catch (std::exception& e) {
                    K2ERROR("======= Test failed with exception [" << e.what() << "] ========");
                    exitcode = -1;
                } catch (...) {
                    K2ERROR("Test failed with unknown exception");
                    exitcode = -1;
                }
            })
            .finally([this] {
                K2INFO("======= Test ended ========");
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

    k2::K23SIClient _client;
    uint64_t txnids = 10000;

public: // tests

// Write a record and read it back via dynamic schema method
seastar::future<> runScenario01() {
    K2INFO("Scenario 01");
    return _client.beginTxn(k2::K2TxnOptions())
    .then([this] (k2::K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (k2::K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);

                    k2::dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<k2::String>("partkey");
                    record.serializeNext<k2::String>("rangekey");
                    record.serializeNext<k2::String>("data1");
                    record.serializeNext<k2::String>("data2");

                    return txnHandle.write<k2::dto::SKVRecord>(record);
                })
                .then([this](auto&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this] () {
                    return _client.getSchema(collname, "schema", k2::K23SIClient::ANY_VERSION);
                })
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);

                    k2::dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<k2::String>("partkey");
                    record.serializeNext<k2::String>("rangekey");

                    return txnHandle.read<k2::dto::SKVRecord>(std::move(record));
                })
                .then([this](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::OK);

                    std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                    K2EXPECT(*partkey, "partkey");
                    K2EXPECT(*rangekey, "rangekey");
                    K2EXPECT(*data1, "data1");
                    K2EXPECT(*data2, "data2");

                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([](auto&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                    return seastar::make_ready_future<>();
                });
        });
    });
}

// Try to issue an end request in parallel with a write
seastar::future<> runScenario02() {
    K2INFO("Scenario 02");
    return _client.beginTxn(k2::K2TxnOptions())
    .then([this] (k2::K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (k2::K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);

                    k2::dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<k2::String>("partkey_02");
                    record.serializeNext<k2::String>("rangekey_02");
                    record.serializeNext<k2::String>("data1");
                    record.serializeNext<k2::String>("data2");

                    _writeFuture = txnHandle.write<k2::dto::SKVRecord>(record).discard_result();
                    return txnHandle.end(true);
                })
                .then([this](auto&& response) {
                    (void) response;
                    K2EXPECT(true, false); // We expect an exception
                    // Need to wait to avoid errors in shutdown
                    return std::move(_writeFuture);
                }).
                handle_exception([this] (auto&& e) {
                    (void) e;
                    K2INFO("Got expected exception in scenario 02");
                    // Need to wait to avoid errors in shutdown
                    return std::move(_writeFuture);
                });
        });
    });
}

// Try to write after an end request
seastar::future<> runScenario03() {
    K2INFO("Scenario 03");
    return _client.beginTxn(k2::K2TxnOptions())
    .then([this] (k2::K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (k2::K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);

                    k2::dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<k2::String>("partkey_s03");
                    record.serializeNext<k2::String>("rangekey_s03");
                    record.serializeNext<k2::String>("data1");
                    record.serializeNext<k2::String>("data2");

                    return txnHandle.write<k2::dto::SKVRecord>(record);
                })
                .then([this](auto&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([this, &txnHandle] () {
                    return txnHandle.end(true);
                })
                .then([this](auto&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                    return _client.getSchema(collname, "schema", 1);
                })
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);

                    k2::dto::SKVRecord record(collname, schemaPtr);
                    record.serializeNext<k2::String>("partkey_s03_2");
                    record.serializeNext<k2::String>("rangekey_s03_2");
                    record.serializeNext<k2::String>("data1");
                    record.serializeNext<k2::String>("data2");

                    return txnHandle.write<k2::dto::SKVRecord>(record);
                })
                .then([](auto&& response) {
                    (void) response;
                    K2EXPECT(false, true); // We expect an exception
                    return seastar::make_ready_future<>();
                })
                .handle_exception( [] (auto&& e) {
                    (void) e;
                    K2INFO("Got expected exception with write after end request");
                    return seastar::make_ready_future<>();
                });
        });
    });
}

// scenario 04 partial update tests for the same schema version
seastar::future<> runScenario04() {
    K2INFO("Scenario 04");
    return _client.beginTxn(k2::K2TxnOptions())
    .then([this] (k2::K2TxnHandle&& txn) {
        return seastar::do_with(
            std::move(txn),
            [this] (k2::K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    // records for read 
                    k2::dto::SKVRecord rdrecord3(collname, schemaPtr);
                    rdrecord3.serializeNext<k2::String>("partkey_s04");
                    rdrecord3.serializeNext<k2::String>("rangekey_s04");
                    k2::dto::SKVRecord rdrecord4(collname, schemaPtr);
                    rdrecord4.serializeNext<k2::String>("partkey_s04");
                    rdrecord4.serializeNext<k2::String>("rangekey_s04");
                    k2::dto::SKVRecord rdrecord5(collname, schemaPtr);
                    rdrecord5.serializeNext<k2::String>("partkey_s04");
                    rdrecord5.serializeNext<k2::String>("rangekey_s04");
                    k2::dto::SKVRecord rdrecord6(collname, schemaPtr);
                    rdrecord6.serializeNext<k2::String>("partkey_s04");
                    rdrecord6.serializeNext<k2::String>("rangekey_s04");
                    k2::dto::SKVRecord rdrecord7(collname, schemaPtr);
                    rdrecord7.serializeNext<k2::String>("partkey_s04");
                    rdrecord7.serializeNext<k2::String>("rangekey_s04");
                    k2::dto::SKVRecord rdrecord8(collname, schemaPtr);
                    rdrecord8.serializeNext<k2::String>("partkey_s04");
                    rdrecord8.serializeNext<k2::String>("rangekey_s04");

                    // records for partial update
                    k2::dto::SKVRecord record0(collname, schemaPtr);
                    k2::dto::SKVRecord record1(collname, schemaPtr);
                    k2::dto::SKVRecord record2(collname, schemaPtr);
                    k2::dto::SKVRecord record3(collname, schemaPtr);
                    k2::dto::SKVRecord record4(collname, schemaPtr);
                    k2::dto::SKVRecord record5(collname, schemaPtr);
                    k2::dto::SKVRecord record6(collname, schemaPtr);
                    k2::dto::SKVRecord record7(collname, schemaPtr);
                    k2::dto::SKVRecord record8(collname, schemaPtr);

                    // initialization
                    record0.serializeNext<k2::String>("partkey_s04");
                    record0.serializeNext<k2::String>("rangekey_s04");
                    record0.serializeNext<k2::String>("data1_v1");
                    record0.serializeNext<k2::String>("data2_v1");
                    
                    // case1: Partial update value fields, with Skipped PartitionKey and RangeKey fields
                    record1.skipNext();
                    record1.skipNext();
                    record1.serializeNext<k2::String>("data1_v2");
                    record1.serializeNext<k2::String>("data2_v2");

                    // case2: Partial update including PartitionKey and RangeKey fields
                    record2.serializeNext<k2::String>("partkey_s04_2");
                    record2.serializeNext<k2::String>("rangekey_s04_2");
                    record2.serializeNext<k2::String>("data3_v1");
                    record2.serializeNext<k2::String>("data4_v1");

                    // case3: partial update every value fields
                    record3.serializeNext<k2::String>("partkey_s04");
                    record3.serializeNext<k2::String>("rangekey_s04");
                    record3.serializeNext<k2::String>("data1_v2");
                    record3.serializeNext<k2::String>("data2_v2");

                    // case4: Partial update some value fields(data2) using field name("f2") to indicate the fieldsToUpdate
                    record4.serializeNext<k2::String>("partkey_s04");
                    record4.serializeNext<k2::String>("rangekey_s04");
                    record4.skipNext();
                    record4.serializeNext<k2::String>("data2_v3");

                    // case5: Partial update some fields(data1) with NULL fieldsToUpdate
                    record5.serializeNext<k2::String>("partkey_s04");
                    record5.serializeNext<k2::String>("rangekey_s04");
                    record5.serializeNext<k2::String>("data1_v3");
                    record5.skipNext();

                    // case6: fieldsToUpdate indicate some fields(data1&2) shall be updated, and it(data2) is skipped in the record
                    record6.serializeNext<k2::String>("partkey_s04");
                    record6.serializeNext<k2::String>("rangekey_s04");
                    record6.serializeNext<k2::String>("data1_v3");
                    record6.skipNext();

                    // case7: fieldsToUpdate indicate some fields(data2) shall not be updated, but record has a value of this field
                    // fieldsToUpdate shall prevail
                    record7.serializeNext<k2::String>("partkey_s04");
                    record7.serializeNext<k2::String>("rangekey_s04");
                    record7.skipNext();
                    record7.serializeNext<k2::String>("data2_v4");

                    // case8: Updates an null field while keeping other null fields null
                    record8.serializeNext<k2::String>("partkey_s04");
                    record8.serializeNext<k2::String>("rangekey_s04");
                    record8.serializeNext<k2::String>("data1_v5");
                    record8.skipNext();

                    return seastar::do_with(
                        std::move(record0),
                        std::move(record1),
                        std::move(record2),
                        std::move(record3),
                        std::move(record4),
                        std::move(record5),
                        std::move(record6),
                        std::move(record7),
                        std::move(record8),
                        std::move(rdrecord3),
                        std::move(rdrecord4),
                        std::move(rdrecord5),
                        std::move(rdrecord6),
                        std::move(rdrecord7),
                        std::move(rdrecord8),
                        [this, &txnHandle] (auto& rec0, auto& rec1, auto& rec2, auto& rec3, auto& rec4, auto& rec5, auto& rec6, auto& rec7, auto& rec8,
                                auto& read3, auto& read4, auto& read5, auto& read6, auto& read7, auto& read8)  {
                        return txnHandle.write<k2::dto::SKVRecord>(rec0)
                        .then([](auto&& response) {
                            K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                        })
                        .then([&] {
                            // case 1
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec1, {0,1,2,3} )
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::KeyNotFound); // because of the serialization added fields
                            });
                        })
                        .then([&] {
                            // case 2
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec2, {2,3} )
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::KeyNotFound);
                            });
                        })
                        .then([&] {
                            // case 3
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec3, {2,3} )
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<k2::dto::SKVRecord>(std::move(read3))
                            .then([](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                                std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();                                
                                K2EXPECT(*partkey, "partkey_s04");
                                K2EXPECT(*rangekey, "rangekey_s04");
                                K2EXPECT(*data1, "data1_v2"); // partial update field
                                K2EXPECT(*data2, "data2_v2"); // partial update field

                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 4
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec4, (std::vector<k2::String>){"f2"})
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<k2::dto::SKVRecord>(std::move(read4))
                            .then([](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                                std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                                K2EXPECT(*partkey, "partkey_s04");
                                K2EXPECT(*rangekey, "rangekey_s04");
                                K2EXPECT(*data1, "data1_v2");
                                K2EXPECT(*data2, "data2_v3"); // partial update field
                                
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 5
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec5, (std::vector<k2::String>){})
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<k2::dto::SKVRecord>(std::move(read5))
                            .then([](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                                std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                                K2EXPECT(*partkey, "partkey_s04");
                                K2EXPECT(*rangekey, "rangekey_s04");
                                K2EXPECT(*data1, "data1_v2"); // "f1" field was not updated successfully
                                K2EXPECT(*data2, "data2_v3");
                                
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 6
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec6, {2,3})
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<k2::dto::SKVRecord>(std::move(read6))
                            .then([](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                                std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                                K2EXPECT(*partkey, "partkey_s04");
                                K2EXPECT(*rangekey, "rangekey_s04");
                                K2EXPECT(*data1, "data1_v3");   // "f1" field was updated to v3 
                                K2EXPECT(*data2, "");           // "f2" field was updated to null
                                
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 7
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec7, (std::vector<k2::String>){"f1"})
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<k2::dto::SKVRecord>(std::move(read7))
                            .then([](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                                std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                                K2EXPECT(*partkey, "partkey_s04");
                                K2EXPECT(*rangekey, "rangekey_s04");
                                K2EXPECT(*data1, ""); 
                                K2EXPECT(*data2, "");
                                
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            // case 8
                            return txnHandle.partialUpdate<k2::dto::SKVRecord>(rec8, (std::vector<k2::String>){"f1"})
                            .then([](auto&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return txnHandle.read<k2::dto::SKVRecord>(std::move(read8))
                            .then([](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                                K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                                std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                                std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                                K2EXPECT(*partkey, "partkey_s04");
                                K2EXPECT(*rangekey, "rangekey_s04");
                                K2EXPECT(*data1, "data1_v5"); // update a null field
                                K2EXPECT(*data2, "");         // this field remains null
                                
                                return seastar::make_ready_future<>();
                            });
                        });
                    }); // end do-with
                });
        }) // end do-with
        .then([] {
            K2INFO("scenario 04 partial update tests passed for the same schema version");
        });
    });
}

// scenario 05 partial update tests for different schema versions
seastar::future<> runScenario05() {
    K2INFO("Scenario 05");
    return seastar::make_ready_future<>()
    .then([this] {
        k2::dto::Schema schema2, schema3;
        schema2.name = "schema";
        schema2.version = 2;
        schema2.fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING, "partition", false, false},
                {k2::dto::FieldType::STRING, "range", false, false},
                {k2::dto::FieldType::STRING, "f2", false, false},
                {k2::dto::FieldType::STRING, "f1", false, false},
        };   
        schema2.setPartitionKeyFieldsByName(std::vector<k2::String>{"partition"});
        schema2.setRangeKeyFieldsByName(std::vector<k2::String> {"range"});

        schema3.name = "schema";
        schema3.version = 3;
        schema3.fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::STRING,  "partition", false, false},
                {k2::dto::FieldType::STRING,  "range", false, false},
                {k2::dto::FieldType::UINT64T, "f3", false, false},
                {k2::dto::FieldType::UINT32T, "f2", false, false},
                {k2::dto::FieldType::STRING,  "f1", false, false},
        };   
        schema3.setPartitionKeyFieldsByName(std::vector<k2::String>{"partition"});
        schema3.setRangeKeyFieldsByName(std::vector<k2::String> {"range"});
    
        return seastar::when_all( _client.createSchema(collname, std::move(schema2)), _client.createSchema(collname, std::move(schema3)) );
    })
    .then([] (auto&& response) mutable {
        auto& [result1, result2] = response;
        auto status1 = result1.get0();
        auto status2 = result2.get0();
        K2EXPECT(status1.status.is2xxOK(), true);
        K2EXPECT(status2.status.is2xxOK(), true);
    })
    .then([this] {
        return _client.beginTxn(k2::K2TxnOptions())
        .then([this] (k2::K2TxnHandle&& txn) {
            return seastar::do_with(
                std::move(txn),
                [this](k2::K2TxnHandle& txnHandle) {
                return _client.getSchema(collname, "schema", 1)
                .then([this, &txnHandle] (auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                                    
                    k2::dto::SKVRecord record0(collname, schemaPtr);
                    // initialization
                    record0.serializeNext<k2::String>("partkey_s05");
                    record0.serializeNext<k2::String>("rangekey_s05");
                    record0.serializeNext<k2::String>("data1_v1");
                    record0.serializeNext<k2::String>("data2_v1");
                    return txnHandle.write<k2::dto::SKVRecord>(record0);
                })
                .then([this](auto&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                // case 1: same fields but different orders, copy pervious data (null fieldsToUpdate)
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord record1(collname, schemaPtr);
                    record1.serializeNext<k2::String>("partkey_s05");
                    record1.serializeNext<k2::String>("rangekey_s05");
                    
                    return txnHandle.partialUpdate<k2::dto::SKVRecord>(record1, (std::vector<k2::String>){})
                    .then([](auto&& response) {
                        K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord read1(collname, schemaPtr);
                    read1.serializeNext<k2::String>("partkey_s05");
                    read1.serializeNext<k2::String>("rangekey_s05");
                    
                    return txnHandle.read<k2::dto::SKVRecord>(std::move(read1));
                })
                .then([this](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                
                    std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                    
                    K2EXPECT(*partkey, "partkey_s05");
                    K2EXPECT(*rangekey, "rangekey_s05");
                    K2EXPECT(*data2, "data2_v1");
                    K2EXPECT(*data1, "data1_v1");
                })
                // case 2: missing a field with same field name but different field type
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord record2(collname, schemaPtr);
                    record2.serializeNext<k2::String>("partkey_s05");
                    record2.serializeNext<k2::String>("rangekey_s05");
                    record2.serializeNext<uint64_t>(64001234);
                    record2.skipNext();
                    record2.serializeNext<k2::String>("data1_v2");
                    return txnHandle.partialUpdate<k2::dto::SKVRecord>(record2, {2,4})
                    .then([](auto&& response) {
                        K2EXPECT(response.status, k2::dto::K23SIStatus::BadParameter);
                    });
                })
                // case 3: add some fields, but leaving them empty
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord record3(collname, schemaPtr);
                    record3.serializeNext<k2::String>("partkey_s05");
                    record3.serializeNext<k2::String>("rangekey_s05");
                    record3.skipNext();
                    record3.serializeNext<uint32_t>(32001234);
                    record3.serializeNext<k2::String>("data1_v2");
                    return txnHandle.partialUpdate<k2::dto::SKVRecord>(record3, (std::vector<k2::String>){"f1", "f2"})
                    .then([](auto&& response) {
                        K2EXPECT(response.status, k2::dto::K23SIStatus::BadParameter);
                    });
                })
                // case 4: add some fields, do not contain some of the pre-existing fields
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord record4(collname, schemaPtr);
                    record4.serializeNext<k2::String>("partkey_s05");
                    record4.serializeNext<k2::String>("rangekey_s05");
                    record4.serializeNext<uint64_t>(64001234);
                    record4.serializeNext<uint32_t>(32001234);
                    record4.skipNext();
                    return txnHandle.partialUpdate<k2::dto::SKVRecord>(record4, (std::vector<k2::String>){"f3", "f2"})
                    .then([](auto&& response) {
                        K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 3);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord read4(collname, schemaPtr);
                    read4.serializeNext<k2::String>("partkey_s05");
                    read4.serializeNext<k2::String>("rangekey_s05");
                    
                    return txnHandle.read<k2::dto::SKVRecord>(std::move(read4));
                })
                .then([this](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                    std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                    std::optional<uint64_t> data3 = response.value.deserializeNext<uint64_t>();
                    std::optional<uint32_t> data2 = response.value.deserializeNext<uint32_t>();
                    std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                    
                    K2EXPECT(*partkey, "partkey_s05");
                    K2EXPECT(*rangekey, "rangekey_s05");
                    K2EXPECT(*data3, 64001234); // partial update field
                    K2EXPECT(*data2, 32001234); // partial update field
                    K2EXPECT(*data1, "data1_v1");
                })
                // case 5: decrease some fields, and update value-fields to null-fields
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord record5(collname, schemaPtr);
                    record5.serializeNext<k2::String>("partkey_s05");
                    record5.serializeNext<k2::String>("rangekey_s05");
                    record5.skipNext();
                    record5.skipNext();
                    return txnHandle.partialUpdate<k2::dto::SKVRecord>(record5, (std::vector<k2::String>){"f2", "f1"})
                    .then([](auto&& response) {
                        K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 2);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord read5(collname, schemaPtr);
                    read5.serializeNext<k2::String>("partkey_s05");
                    read5.serializeNext<k2::String>("rangekey_s05");
                    
                    return txnHandle.read<k2::dto::SKVRecord>(std::move(read5));
                })
                .then([this](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                    std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                                        
                    K2EXPECT(*partkey, "partkey_s05");
                    K2EXPECT(*rangekey, "rangekey_s05");
                    K2EXPECT(*data2, ""); // parital update value-field to null
                    K2EXPECT(*data1, ""); // parital update value-field to null
                })
                // case 6: update fields from null to null
                .then([&] {
                    return _client.getSchema(collname, "schema", 1);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord record6(collname, schemaPtr);
                    record6.serializeNext<k2::String>("partkey_s05");
                    record6.serializeNext<k2::String>("rangekey_s05");
                    record6.skipNext();
                    record6.skipNext();
                    return txnHandle.partialUpdate<k2::dto::SKVRecord>(record6, (std::vector<k2::String>){})
                    .then([](auto&& response) {
                        K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    });
                })
                .then([&] {
                    return _client.getSchema(collname, "schema", 1);
                })
                .then([&](auto&& response) {
                    auto& [status, schemaPtr] = response;
                    K2EXPECT(status.is2xxOK(), true);
                    
                    k2::dto::SKVRecord read6(collname, schemaPtr);
                    read6.serializeNext<k2::String>("partkey_s05");
                    read6.serializeNext<k2::String>("rangekey_s05");
                    
                    return txnHandle.read<k2::dto::SKVRecord>(std::move(read6));
                })
                .then([this](k2::ReadResult<k2::dto::SKVRecord>&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
                                
                    std::optional<k2::String> partkey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> rangekey = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data1 = response.value.deserializeNext<k2::String>();
                    std::optional<k2::String> data2 = response.value.deserializeNext<k2::String>();
                                        
                    K2EXPECT(*partkey, "partkey_s05");
                    K2EXPECT(*rangekey, "rangekey_s05");
                    K2EXPECT(*data1, ""); // parital update null-field to null
                    K2EXPECT(*data2, ""); // parital update null-field to null
                });
            }) // end do-with txnHandle
            .then([] {
                K2INFO("scenario 05 partial update tests passed for different schema versions");
            });
        });
    });
}

};  // class SKVClientTest

int main(int argc, char** argv) {
    k2::App app("SKVClientTest");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<k2::TSO_ClientLib>(0s);
    app.addApplet<SKVClientTest>();
    return app.start(argc, argv);
}
