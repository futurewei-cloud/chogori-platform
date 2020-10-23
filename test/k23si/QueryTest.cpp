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

// Integration tests for SKV Query operations
// These assume the "k23si_query_pagination_limit" server option is set to 2
// and two partitions
class QueryTest {

public:  // application lifespan
    QueryTest() : _client(k2::K23SIClientConfig()) { K2INFO("ctor");}
    ~QueryTest(){ K2INFO("dtor");}

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2INFO("start");

        _testFuture = seastar::make_ready_future()
        .then([this] () {
            return _client.start();
        })
        .then([this] {
            K2INFO("Creating test collection...");
            std::vector<k2::String> rangeEnds;
            rangeEnds.push_back(k2::dto::FieldToKeyString<k2::String>("d"));
            rangeEnds.push_back("");

            return _client.makeCollection(collname, std::move(rangeEnds));
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
            };

            schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"partition"});
            schema.setRangeKeyFieldsByName(std::vector<k2::String> {"range"});

            return _client.createSchema(collname, std::move(schema));
        })
        .then([] (auto&& result) {
            K2EXPECT(result.status.is2xxOK(), true);
        })
        .then([this] { return runSetup(); })
        .then([this] { return runScenario01(); })
        .then([this] { return runScenario02(); })
        .then([this] { return runScenario03(); })
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

seastar::future<> doQuery(const k2::String& start, const k2::String& end, int32_t limit, bool reverse, 
                          uint32_t expectedRecords, uint32_t expectedPaginations, 
                          k2::Status expectedStatus=k2::dto::K23SIStatus::OK,
                          std::vector<k2::String> projection=std::vector<k2::String>()) {
    K2DEBUG("doQuery from " << start " to: " << end);
    return _client.beginTxn(k2::K2TxnOptions{})
    .then([this] (k2::K2TxnHandle&& t) {
        txn = std::move(t);
        return _client.createQuery(collname, "schema");
    })
    .then([this, start, end, limit, reverse, expectedRecords, expectedPaginations, expectedStatus, projection] (auto&& response) {
        K2EXPECT(response.status.is2xxOK(), true);
        query = std::move(response.query);
        if (start != "" || !reverse) {
            query.startScanRecord.serializeNext<k2::String>(start);
            query.startScanRecord.serializeNext<k2::String>("");
        }
        if (end != "" || reverse) {
            query.endScanRecord.serializeNext<k2::String>(end);
            query.endScanRecord.serializeNext<k2::String>("");
        }
        query.setLimit(limit);
        query.setReverseDirection(reverse);
        query.addProjection(projection);

        return seastar::do_with(std::vector<std::vector<k2::dto::SKVRecord>>(), (uint32_t)0, false, 
        [this, expectedRecords, expectedPaginations, expectedStatus, projection] (
                std::vector<std::vector<k2::dto::SKVRecord>>& result_set, uint32_t& count, bool& done) {
            return seastar::do_until(
                [this, &done] () { return done; },
                [this, &result_set, &count, &done, expectedStatus] () {
                    return txn.query(query)
                    .then([this, &result_set, &count, &done, expectedStatus] (auto&& response) {
                        K2EXPECT(response.status, expectedStatus);
                        done = response.status.is2xxOK() ? query.isDone() : true;
                        ++count;
                        result_set.push_back(std::move(response.records));
                    });
            })
            .then([&result_set, &count, expectedPaginations, expectedRecords, expectedStatus, projection] () {
                if (!expectedStatus.is2xxOK()) {
                    return seastar::make_ready_future();
                }

                uint32_t record_count = 0;
                for (std::vector<k2::dto::SKVRecord>& set : result_set) {
                    record_count += set.size();

                    // verify projection skvrecord
                    if (projection.size() != 0) {
                        for (k2::dto::SKVRecord& rec : set) {
                            std::optional<k2::String> partition = rec.deserializeNext<k2::String>();
                            std::optional<k2::String> range = rec.deserializeNext<k2::String>();
                        
                            // partitionKey verification
                            auto pIt = std::find(projection.begin(), projection.end(), "partition");
                            if (pIt == projection.end()) {
                                K2ASSERT(!partition, "Exclude this field"); 
                            } else {
                                K2ASSERT(partition, "SKVRecord should have got this field");
                            }
                            // rangeKey verification
                            auto rIt = std::find(projection.begin(), projection.end(), "range");
                            if (rIt == projection.end()) {
                                K2ASSERT(!range, "Exclude this field"); 
                            } else {
                                K2ASSERT(range, "SKVRecord should have got this field");
                            }
                        }
                    }
                }
                K2EXPECT(record_count, expectedRecords);
                K2EXPECT(count, expectedPaginations);
                return seastar::make_ready_future<>();
            });
        });
    });
}

// Write six records ("a"-"f"), three to each partition
seastar::future<> runSetup() {
    K2INFO("QueryTest setup");
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
        K2EXPECT(status.is2xxOK(), true);

        std::vector<seastar::future<>> write_futs;
        std::vector<k2::String> partKeys = {"a", "b", "c", "d", "e", "f"};

        for (const k2::String& key : partKeys) {
            k2::dto::SKVRecord record(collname, schemaPtr);
            record.serializeNext<k2::String>(key);
            record.serializeNext<k2::String>("");
            write_futs.push_back(txn.write<k2::dto::SKVRecord>(record)
                .then([] (auto&& response) {
                    K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
            );
        }

        return seastar::when_all_succeed(write_futs.begin(), write_futs.end());
    })
    .then([this] () {
        return txn.end(true);
    })
    .then([](auto&& response) {
        K2EXPECT(response.status, k2::dto::K23SIStatus::OK);
        return seastar::make_ready_future<>();
    });
}

// Simple forward scan queries with no filter or projection
seastar::future<> runScenario01() {
    K2INFO("runScenario01");

    K2INFO("Single partition single page result");
    return doQuery("a", "c", -1, false, 2, 1)
    .then([this] () {
        K2INFO("Single partition with limit");
        return doQuery("a", "d", 1, false, 1, 1);
    })
    .then([this] () {
        K2INFO("Single partition no results");
        return doQuery("ab", "abz", -1, false, 0, 1);
    })
    .then([this] () {
        K2INFO("Single partition with end key == partition end");
        return doQuery("a", "d", -1, false, 3, 2);
    })
    .then([this] () {
        K2INFO("Multi partition with limit");
        return doQuery("a", "", 5, false, 5, 3);
    })
    .then([this] () {
        K2INFO("Multi partition terminated by end key");
        return doQuery("a", "dz", -1, false, 4, 3);
    })
    .then([this] () {
        K2INFO("Multi partition full scan");
        return doQuery("", "", -1, false, 6, 4);
    });
}

// Query projection cases
seastar::future<> runScenario02() {
    K2INFO("runScenario02");

    K2INFO("Projection reads for giving fields");
    return doQuery("a", "c", -1, false, 2, 1, k2::dto::K23SIStatus::OK, {"partition"});
}

// Query conflict cases
seastar::future<> runScenario03() {
    K2INFO("runScenario03");

    K2INFO("Starting write in range is blocked by readCache test");
    k2::K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (k2::K2TxnHandle&& t) {
        writeTxn = std::move(t);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return doQuery("a", "d", -1, false, 3, 2);
    })
    .then([this] () {
        return _client.getSchema(collname, "schema", 1);
    })
    .then([this] (auto&& response) {
        auto& [status, schemaPtr] = response;
        K2EXPECT(status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("bb");
        record.serializeNext<k2::String>("");
        return writeTxn.write<k2::dto::SKVRecord>(record);
    })
    .then([this] (auto&& response) {
        K2EXPECT(response.status, k2::dto::K23SIStatus::AbortRequestTooOld);
        return writeTxn.end(false);
    })
    .then([] (auto&& response) {
        K2EXPECT(response.status.is2xxOK(), true);
        return seastar::make_ready_future<>();
    })
    .then([this, options] () mutable {
        K2INFO("Starting test for query loses PUSH");
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
        K2EXPECT(status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("az");
        record.serializeNext<k2::String>("");
        return writeTxn.write<k2::dto::SKVRecord>(record);
    })
    .then([this] (auto&& response) {
        K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        return doQuery("a", "d", -1, false, 3, 2, k2::dto::K23SIStatus::AbortConflict);
    })
    .then([this] () {
        return writeTxn.end(true);
    })
    .then([] (auto&& response) {
        K2EXPECT(response.status.is2xxOK(), true);
        return seastar::make_ready_future<>();
    })
    .then([this, options] () mutable {
        K2INFO("Starting test for query wins PUSH");
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
        K2EXPECT(status.is2xxOK(), true);

        k2::dto::SKVRecord record(collname, schemaPtr);
        record.serializeNext<k2::String>("bz");
        record.serializeNext<k2::String>("");
        return writeTxn.write<k2::dto::SKVRecord>(record);
    })
    .then([this] (auto&& response) {
        K2EXPECT(response.status, k2::dto::K23SIStatus::Created);
        return seastar::make_ready_future<>();
    })
    .then([this] () {
        // Records at this point in the range are: "a" "az" "b" "c" and WI for "bz"
        return doQuery("a", "d", -1, false, 4, 2);
    })
    .then([this] () {
        return writeTxn.end(true);
    })
    .then([] (auto&& response) {
        K2EXPECT(response.status, k2::dto::K23SIStatus::OperationNotAllowed);
        return seastar::make_ready_future<>();
    });
}

};  // class QueryTest

int main(int argc, char** argv) {
    k2::App app("SKVClientTest");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<k2::TSO_ClientLib>(0s);
    app.addApplet<QueryTest>();
    return app.start(argc, argv);
}
