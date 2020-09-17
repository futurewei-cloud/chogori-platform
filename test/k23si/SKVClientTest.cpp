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
                
                return _client.cpo_client.createSchema(collname, std::move(schema));
            })
            .then([] (auto&& status) {
                K2EXPECT(status.is2xxOK(), true);
            })
            .then([this] { return runScenario01(); })
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
