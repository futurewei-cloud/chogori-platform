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
#include "Log.h"
using namespace k2;
namespace k2e = k2::dto::expression;

const char* collname = "k23si_test_collection";

// Integration tests for SKV Query operations
// These assume the "k23si_query_pagination_limit" server option is set to 2
// and two partitions
class QueryTest {

public:  // application lifespan
    QueryTest() : _client(k2::K23SIClientConfig()) { K2LOG_I(log::k23si, "ctor");}
    ~QueryTest(){ K2LOG_I(log::k23si, "dtor");}

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2LOG_I(log::k23si, "start");

        _testFuture = seastar::make_ready_future()
        .then([this] () {
            return _client.start();
        })
        .then([this] {
            K2LOG_I(log::k23si, "Creating test collection...");
            std::vector<k2::String> rangeEnds;
            rangeEnds.push_back(k2::dto::FieldToKeyString<k2::String>("default") +
                                k2::dto::FieldToKeyString<k2::String>("d"));
            rangeEnds.push_back("");

            return _client.makeCollection(collname, std::move(rangeEnds));
        })
        .then([](auto&& status) {
            K2EXPECT(log::k23si, status.is2xxOK(), true);
        })
        .then([this] () {
            k2::dto::Schema schema;
            schema.name = "schema";
            schema.version = 1;
            schema.fields = std::vector<k2::dto::SchemaField> {
                    {k2::dto::FieldType::STRING, "partition", false, false},
                    {k2::dto::FieldType::STRING, "partition2", false, false},
                    {k2::dto::FieldType::STRING, "range", false, false},
                    {k2::dto::FieldType::INT32T, "data1", false, false},
                    {k2::dto::FieldType::INT32T, "data2", false, false}
            };

            schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"partition", "partition2"});
            schema.setRangeKeyFieldsByName(std::vector<k2::String> {"range"});

            return _client.createSchema(collname, std::move(schema));
        })
        .then([] (auto&& result) {
            K2EXPECT(log::k23si, result.status.is2xxOK(), true);
        })
        .then([this] { return runEmptyScenario(); })
        .then([this] { return runSetup(); })
        .then([this] { return runScenario01(); })
        .then([this] { return runScenario02(); })
        .then([this] { return runScenario03(); })
        .then([this] { return runScenario04(); })
        .then([this] { return runScenario05(); })
        .then([this] { return runScenario06(); })
        .then([this] { return runScenario07(); })
        .then([this] { return runScenario08(); })
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

        return seastar::make_ready_future();
    }

private:
    int exitcode = -1;

    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::future<> _writeFuture = seastar::make_ready_future();

    k2::K23SIClient _client;
    k2::K2TxnHandle txn;
    k2::K2TxnHandle writeTxn;
    k2::Query query;

public: // tests

seastar::future<std::vector<std::vector<k2::dto::SKVRecord>>>
doQuery(const k2::String& start, const k2::String& end, int32_t limit, bool reverse,
                          uint32_t expectedRecords, uint32_t expectedPaginations,
                          k2::Status expectedStatus=k2::dto::K23SIStatus::OK,
                          k2e::Expression filterExpression=k2e::Expression{},
                          std::vector<k2::String> projection=std::vector<k2::String>(),
                          bool doPrefixScan = false) {
    K2LOG_D(log::k23si, "doQuery from {} to {}", start, end);
    return _client.beginTxn(k2::K2TxnOptions{})
    .then([this] (k2::K2TxnHandle&& t) {
        txn = std::move(t);
        return _client.createQuery(collname, "schema");
    })
    .then([this, start, end, limit, reverse, expectedRecords, expectedPaginations, expectedStatus,
                filterExpression=std::move(filterExpression),
                projection=std::move(projection),
                doPrefixScan] (auto&& response) mutable {
        K2EXPECT(log::k23si, response.status.is2xxOK(), true);
        query = std::move(response.query);

        // Fully specified scan or full schema scan
        if (!doPrefixScan) {
            if (start != "" || !reverse) {
                query.startScanRecord.serializeNext<k2::String>("default");
                query.startScanRecord.serializeNext<k2::String>(start);
                query.startScanRecord.serializeNext<k2::String>("");
            }
            if (end != "" || reverse) {
                query.endScanRecord.serializeNext<k2::String>("default");
                query.endScanRecord.serializeNext<k2::String>(end);
                query.endScanRecord.serializeNext<k2::String>("");
            }
        } else { // Prefix scan where only first key fields is set
            query.startScanRecord.serializeNext<k2::String>("default");
            query.endScanRecord.serializeNext<k2::String>("default");
        }

        query.setLimit(limit);
        query.setReverseDirection(reverse);
        query.addProjection(projection);
        query.setFilterExpression(std::move(filterExpression));

        return seastar::do_with(std::vector<std::vector<k2::dto::SKVRecord>>(), (uint32_t)0, false,
        [this, expectedRecords, expectedPaginations, expectedStatus, projection] (
                std::vector<std::vector<k2::dto::SKVRecord>>& result_set, uint32_t& count, bool& done) {
            return seastar::do_until(
                [this, &done] () { return done; },
                [this, &result_set, &count, &done, expectedStatus] () {
                    return txn.query(query)
                    .then([this, &result_set, &count, &done, expectedStatus] (auto&& response) {
                        K2EXPECT(log::k23si, response.status, expectedStatus);
                        done = response.status.is2xxOK() ? query.isDone() : true;
                        ++count;
                        result_set.push_back(std::move(response.records));
                    });
            })
            .then([&result_set, &count, expectedPaginations, expectedRecords, expectedStatus] () {
                if (!expectedStatus.is2xxOK()) {
                    return seastar::make_ready_future<std::vector<std::vector<k2::dto::SKVRecord>>>(std::move(result_set));
                }

                uint32_t record_count = 0;
                for (std::vector<k2::dto::SKVRecord>& set : result_set) {
                    record_count += set.size();
                }
                K2EXPECT(log::k23si, record_count, expectedRecords);
                K2EXPECT(log::k23si, count, expectedPaginations);
                return seastar::make_ready_future<std::vector<std::vector<k2::dto::SKVRecord>>>(std::move(result_set));
            });
        });
    });
}

// Write six records with partition2 keys ("a"-"f"), three to each partition
// Write additional two records with non-default partition1 key
seastar::future<> runSetup() {
    K2LOG_I(log::k23si, "QueryTest setup");
    k2::K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (k2::K2TxnHandle&& t) {
        txn = std::move(t);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return _client.getSchema(collname, "schema", 1);
    })
    .then([this] (auto&& response) {
        auto& [status, schemaPtr] = response;
        K2EXPECT(log::k23si, status.is2xxOK(), true);

        std::vector<seastar::future<>> write_futs;
        std::vector<k2::String> partKeys = {"a", "b", "c", "d", "e", "f"};
        int32_t data = 0;

        for (const k2::String& key : partKeys) {
            k2::dto::SKVRecord record(collname, schemaPtr);
            record.serializeNext<k2::String>("default");
            record.serializeNext<k2::String>(key);
            record.serializeNext<k2::String>("");
            record.serializeNext<int32_t>(data);
            record.serializeNext<int32_t>(data);
            write_futs.push_back(txn.write<k2::dto::SKVRecord>(record)
                .then([] (auto&& response) {
                    K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
            );
            data++;
        }

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("nondefault");
        record.serializeNext<k2::String>("a");
        record.serializeNext<k2::String>("");
        record.serializeNull();
        record.serializeNext<int32_t>(777);
        write_futs.push_back(txn.write<k2::dto::SKVRecord>(record)
            .then([] (auto&& response) {
                K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::Created);
                return seastar::make_ready_future<>();
            })
        );

        k2::dto::SKVRecord record2(collname, schemaPtr);
        record2.serializeNext<k2::String>("nondefault");
        record2.serializeNext<k2::String>("z");
        record2.serializeNext<k2::String>("");
        record2.serializeNull();
        record2.serializeNext<int32_t>(777);
        write_futs.push_back(txn.write<k2::dto::SKVRecord>(record2)
            .then([] (auto&& response) {
                K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::Created);
                return seastar::make_ready_future<>();
            })
        );

        return seastar::when_all_succeed(write_futs.begin(), write_futs.end());
    })
    .then([this] () {
        return txn.end(true);
    })
    .then([](auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::OK);
        return seastar::make_ready_future<>();
    });
}

// Simple forward scan queries with no filter or projection
seastar::future<> runScenario01() {
    K2LOG_I(log::k23si, "runScenario01");

    K2LOG_I(log::k23si, "Single partition single page result");
    return doQuery("a", "c", -1, false, 2, 1).discard_result()
    .then([this] () {
        K2LOG_I(log::k23si, "Single partition with limit");
        return doQuery("a", "d", 1, false, 1, 1).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Single partition no results");
        return doQuery("ab", "abz", -1, false, 0, 1).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Single partition with end key == partition end");
        return doQuery("a", "d", -1, false, 3, 2).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Multi partition with limit");
        return doQuery("a", "", 5, false, 5, 3).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Multi partition terminated by end key");
        return doQuery("a", "dz", -1, false, 4, 3).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Start and end keys the same");
        return doQuery("b", "b", -1, false, 1, 1).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Multi partition full scan");
        return doQuery("", "", -1, false, 8, 5).discard_result();
    });
}

// Simple reverse scan query for empty records.
seastar::future<> runEmptyScenario(){
    K2LOG_I(log::k23si, "runEmptyScenario");
    K2LOG_I(log::k23si, "Single partition empty records no result");
    return doQuery("c", "a", 1, true, 0, 1).discard_result();
}

// Simple reverse scan queries with no filter or projection
seastar::future<> runScenario02() {
    K2LOG_I(log::k23si, "runScenario02");

    K2LOG_I(log::k23si, "Single partition single page result");
    return doQuery("c", "a", -1, true, 2, 1).discard_result()
    .then([this] {
        K2LOG_I(log::k23si, "Single partition with limit");
        return doQuery("d", "a", 1, true, 1, 1).discard_result();
    })
    .then([this] {
        K2LOG_I(log::k23si, "Single partition no results");
        return doQuery("abz", "ab", -1, true, 0, 1).discard_result();
    })
    .then([this] {
        K2LOG_I(log::k23si, "Single partition with end key == partition start");
        return doQuery("f", "d", -1, true, 2, 1).discard_result();
    })
    .then([this] {
        K2LOG_I(log::k23si, "Single partition with end key right smaller than partition start");
        return doQuery("f", "c", -1, true, 3, 3).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Multi partition with limit");
        return doQuery("f", "", 5, true, 5, 3).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Start and end keys the same");
        return doQuery("b", "b", -1, false, 1, 1).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Multi partition terminated by end key");
        return doQuery("f", "baby", -1, true, 4, 3).discard_result();
    })
    .then([this] () {
        K2LOG_I(log::k23si, "Multi partition full scan");
        return doQuery("", "", -1, true, 8, 5)
        .then([](auto&& response) {
            std::vector<std::vector<k2::dto::SKVRecord>>& result_set = response;
            // First two returned records have partition1="nondefault"
            std::deque<k2::String> partKeys = {"z", "a", "f", "e", "d", "c", "b", "a"};

            for (std::vector<k2::dto::SKVRecord>& set : result_set) {
                for (k2::dto::SKVRecord& rec : set) {
                    std::optional<k2::String> part1 = rec.deserializeNext<k2::String>();
                    std::optional<k2::String> part2 = rec.deserializeNext<k2::String>();
                    std::optional<k2::String> range = rec.deserializeNext<k2::String>();
                    K2LOG_I(log::k23si, "partitonKey: {}, rkey: {}", *part2, *range);
                    K2EXPECT(log::k23si, *part2, partKeys.front());
                    partKeys.pop_front();
                }
            }
        });
    });
}

// Query projection cases
seastar::future<> runScenario03() {
    K2LOG_I(log::k23si, "runScenario03");

    K2LOG_I(log::k23si, "Projection reads for the giving field");
    return doQuery("a", "c", -1, false, 2, 1, k2::dto::K23SIStatus::OK, k2e::Expression{}, {"partition2"})
    .then([](auto&& response) {
        std::vector<std::vector<k2::dto::SKVRecord>>& result_set = response;
        uint32_t record_count = 0;
        std::deque<k2::String> partKeys = {"a", "b"};

        K2EXPECT(log::k23si, result_set.size(), 1);
        for (std::vector<k2::dto::SKVRecord>& set : result_set) {
            record_count += set.size();

            // verify projection skvrecord
            for (k2::dto::SKVRecord& rec : set) {
                std::optional<k2::String> part1 = rec.deserializeNext<k2::String>();
                std::optional<k2::String> part2 = rec.deserializeNext<k2::String>();
                std::optional<k2::String> range = rec.deserializeNext<k2::String>();
                K2ASSERT(log::k23si, part2, "SKVRecord should have got this field");
                K2EXPECT(log::k23si, *part2, partKeys.front());
                partKeys.pop_front();
                K2ASSERT(log::k23si, !range, "Exclude this field");
            }
        }
        K2EXPECT(log::k23si, record_count, 2);
    })
    .then([this] {
        K2LOG_I(log::k23si, "Full scan with projection over records with null field");
        return doQuery("", "", -1, false, 8, 5, k2::dto::K23SIStatus::OK, k2e::Expression{},
                       {"data2"}).discard_result();
    })
    .then([this] {
        K2LOG_I(log::k23si, "Project a field that is not part of the schema");
        return doQuery("a", "c", -1, false, 2, 1, k2::dto::K23SIStatus::OK, k2e::Expression{},
                       {"partition2", "balance", "age"})
        .then([](auto&& response) {
            std::vector<std::vector<k2::dto::SKVRecord>>& result_set = response;
            uint32_t record_count = 0;
            std::deque<k2::String> partKeys = {"a", "b"};

            K2EXPECT(log::k23si, result_set.size(), 1);
            for (std::vector<k2::dto::SKVRecord>& set : result_set) {
                record_count += set.size();

                // verify projection skvrecord
                for (k2::dto::SKVRecord& rec : set) {
                    std::optional<k2::String> part1 = rec.deserializeNext<k2::String>();
                    std::optional<k2::String> part2 = rec.deserializeNext<k2::String>();
                    std::optional<k2::String> range = rec.deserializeNext<k2::String>();
                    K2ASSERT(log::k23si, part2, "SKVRecord should have got this field");
                    K2EXPECT(log::k23si, *part2, partKeys.front());
                    partKeys.pop_front();
                    K2ASSERT(log::k23si, !range, "Exclude this field");
                }
            }
            K2EXPECT(log::k23si, record_count, 2);
        });
    });
}

// Query filter tests. Filter expressions have extensive unit tests, so
// only testing end-to-end functionality here
seastar::future<> runScenario04() {
    K2LOG_I(log::k23si, "runScenario04");
    return seastar::make_ready_future()
    .then([this] () {
        // Simple Equals filter, all records should be returned
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueReference("data2"));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        return doQuery("", "", -1, false, 6, 4, k2::dto::K23SIStatus::OK, std::move(filter)).discard_result();
    })
    .then([this] () {
        // Simple Equals filter, only one record returned but all partitions accessed
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueLiteral<int32_t>(1));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        return doQuery("", "", -1, false, 1, 2, k2::dto::K23SIStatus::OK, std::move(filter))
        .then([this] (auto&& result_set) {
            k2::dto::SKVRecord& rec = result_set[0][0];
            std::optional<k2::String> part1 = rec.deserializeNext<k2::String>();
            std::optional<k2::String> key = rec.deserializeNext<k2::String>();
            K2EXPECT(log::k23si, *key, "b");
        });
    })
    .then([this] () {
        // Equals filter with type promotion, only one record returned but all partitions accessed
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueLiteral<int64_t>(1));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        return doQuery("", "", -1, false, 1, 2, k2::dto::K23SIStatus::OK, std::move(filter))
        .then([this] (auto&& result_set) {
            k2::dto::SKVRecord& rec = result_set[0][0];
            std::optional<k2::String> part1 = rec.deserializeNext<k2::String>();
            std::optional<k2::String> key = rec.deserializeNext<k2::String>();
            K2EXPECT(log::k23si, *key, "b");
        });
    })
    .then([this] () {
        // Mismatched types, no records returned
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueLiteral<k2::String>("1"));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        return doQuery("", "", -1, false, 0, 2, k2::dto::K23SIStatus::OK, std::move(filter))
        .discard_result();
    })
    .then([this] () {
        // Failure case of malformed filter
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        return doQuery("", "", -1, false, 0, 1, k2::dto::K23SIStatus::OperationNotAllowed, std::move(filter))
        .discard_result();
    });
}

// Forward and reverse prefix scan. For example, a schema with two
// partition keys. Start and end keys specify the first key but not the
// second, with the intent that all records with the first key equals are returned,
// and the second key can be any value. This requires special handling on the server
// because the start and end records have the same key string.
seastar::future<> runScenario05() {
    K2LOG_I(log::k23si, "runScenario05");

    K2LOG_I(log::k23si, "Forward prefix scan test");
    return doQuery("", "", -1, false, 6, 4, k2::dto::K23SIStatus::OK,
                   k2e::Expression{}, std::vector<k2::String>(), true /* prefix only */).discard_result()
    .then([this] () {
        K2LOG_I(log::k23si, "Reverse prefix scan");
        return doQuery("", "", -1, true, 6, 4, k2::dto::K23SIStatus::OK,
                       k2e::Expression{}, std::vector<k2::String>(), true /* prefix only */).discard_result();
    });
}

// Query conflict cases
seastar::future<> runScenario06() {
    K2LOG_I(log::k23si, "runScenario06");

    K2LOG_I(log::k23si, "Starting write in range is blocked by readCache test");
    k2::K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (k2::K2TxnHandle&& t) {
        writeTxn = std::move(t);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return doQuery("a", "d", -1, false, 3, 2).discard_result();
    })
    .then([this] () {
        return _client.getSchema(collname, "schema", 1);
    })
    .then([this] (auto&& response) {
        auto& [status, schemaPtr] = response;
        K2EXPECT(log::k23si, status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("default");
        record.serializeNext<k2::String>("bb");
        record.serializeNext<k2::String>("");
        return writeTxn.write<k2::dto::SKVRecord>(record);
    })
    .then([this] (auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::AbortRequestTooOld);
        return writeTxn.end(false);
    })
    .then([] (auto&& response) {
        K2EXPECT(log::k23si, response.status.is2xxOK(), true);
        return seastar::make_ready_future<>();
    })
    .then([this, options] () mutable {
        K2LOG_I(log::k23si, "Starting test for query loses PUSH");
        options.priority = k2::dto::TxnPriority::High;
        return _client.beginTxn(options);
    })
    .then([this] (k2::K2TxnHandle&& t) {
        writeTxn = std::move(t);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return _client.getSchema(collname, "schema", 1);
    })
    .then([this] (auto&& response) {
        auto& [status, schemaPtr] = response;
        K2EXPECT(log::k23si, status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("default");
        record.serializeNext<k2::String>("az");
        record.serializeNext<k2::String>("");
        return writeTxn.write<k2::dto::SKVRecord>(record);
    })
    .then([this] (auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::Created);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return doQuery("a", "d", -1, false, 3, 2, k2::dto::K23SIStatus::AbortConflict).discard_result();
    })
    .then([this] () {
        return writeTxn.end(true);
    })
    .then([] (auto&& response) {
        K2EXPECT(log::k23si, response.status.is2xxOK(), true);
        return seastar::make_ready_future<>();
    })
    .then([this, options] () mutable {
        K2LOG_I(log::k23si, "Starting test for query wins PUSH");
        return _client.beginTxn(options);
    })
    .then([this] (k2::K2TxnHandle&& t) {
        writeTxn = std::move(t);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return _client.getSchema(collname, "schema", 1);
    })
    .then([this] (auto&& response) {
        auto& [status, schemaPtr] = response;
        K2EXPECT(log::k23si, status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("default");
        record.serializeNext<k2::String>("bz");
        record.serializeNext<k2::String>("");
        return writeTxn.write<k2::dto::SKVRecord>(record);
    })
    .then([this] (auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::Created);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        // Records at this point in the range are: "a" "az" "b" "c" and WI for "bz"
        return doQuery("a", "d", -1, false, 4, 2).discard_result();
    })
    .then([this] () {
        return writeTxn.end(true);
    })
    .then([] (auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::OperationNotAllowed);
        return seastar::make_ready_future<>();
    });
}

// Read an erased record via query
seastar::future<> runScenario07() {
    K2LOG_I(log::k23si, "runScenario07");

    K2LOG_I(log::k23si, "Read an erased record via query");
    k2::K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (k2::K2TxnHandle&& t) {
        writeTxn = std::move(t);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return _client.getSchema(collname, "schema", 1);
    })
    .then([this] (auto&& response) {
        auto& [status, schemaPtr] = response;
        K2EXPECT(log::k23si, status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("default");
        record.serializeNext<k2::String>("scenario07");
        record.serializeNext<k2::String>("");
        return writeTxn.write<k2::dto::SKVRecord>(record);
    })
    .then([this] (auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::Created);
        return _client.getSchema(collname, "schema", 1);
    })
    .then([this] (auto&& response) {
        auto& [status, schemaPtr] = response;
        K2EXPECT(log::k23si, status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("default");
        record.serializeNext<k2::String>("scenario07");
        record.serializeNext<k2::String>("");
        return writeTxn.write(record, true /* isDelete */);
    })
    .then([this] (auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::Created);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return writeTxn.end(true);
    })
    .then([] (auto&& response) {
        K2EXPECT(log::k23si, response.status, k2::dto::K23SIStatus::OK);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return doQuery("scenario07", "scenario07", -1, false, 0, 1).discard_result();
    });
}

// scenario04 but in reverse scan
seastar::future<> runScenario08() {
    K2LOG_I(log::k23si, "runScenario08");
    return seastar::make_ready_future()
    .then([this] () {
        // Simple Equals filter, all records should be returned
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueReference("data2"));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        K2LOG_I(log::k23si, "runScenario08-1");
        return doQuery("", "", -1, true, 6, 4, k2::dto::K23SIStatus::OK, std::move(filter)).discard_result();
    })
    .then([this] () {
        // Simple Equals filter, only one record returned but all partitions accessed
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueLiteral<int32_t>(1));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        K2LOG_I(log::k23si, "runScenario08-2");
        return doQuery("", "", -1, true, 1, 2, k2::dto::K23SIStatus::OK, std::move(filter))
        .then([this] (auto&& result_set) {
            k2::dto::SKVRecord& rec = result_set[0][0];
            std::optional<k2::String> part1 = rec.deserializeNext<k2::String>();
            std::optional<k2::String> key = rec.deserializeNext<k2::String>();
            K2EXPECT(log::k23si, *key, "b");
        });
    })
    .then([this] () {
        // Equals filter with type promotion, only one record returned but all partitions accessed
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueLiteral<int64_t>(1));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        K2LOG_I(log::k23si, "runScenario08-3");
        return doQuery("", "", -1, true, 1, 2, k2::dto::K23SIStatus::OK, std::move(filter))
        .then([this] (auto&& result_set) {
            k2::dto::SKVRecord& rec = result_set[0][0];
            std::optional<k2::String> part1 = rec.deserializeNext<k2::String>();
            std::optional<k2::String> key = rec.deserializeNext<k2::String>();
            K2EXPECT(log::k23si, *key, "b");
        });
    })
    .then([this] () {
        // Mismatched types, no records returned
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        values.emplace_back(k2e::makeValueLiteral<k2::String>("1"));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        K2LOG_I(log::k23si, "runScenario08-4");
        return doQuery("", "", -1, true, 0, 2, k2::dto::K23SIStatus::OK, std::move(filter))
        .discard_result();
    })
    .then([this] () {
        // Failure case of malformed filter
        std::vector<k2e::Value> values;
        std::vector<k2e::Expression> exps;
        values.emplace_back(k2e::makeValueReference("data1"));
        k2e::Expression filter = k2e::makeExpression(k2e::Operation::EQ, std::move(values), std::move(exps));
        K2LOG_I(log::k23si, "runScenario08-5");
        return doQuery("", "", -1, true, 0, 1, k2::dto::K23SIStatus::OperationNotAllowed, std::move(filter))
        .discard_result();
    });
}

// TODO: add test Scenario to deal with query request while change the partition map

};  // class QueryTest

int main(int argc, char** argv) {
    k2::App app("SKVClientTest");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<k2::TSO_ClientLib>();
    app.addApplet<QueryTest>();
    return app.start(argc, argv);
}
