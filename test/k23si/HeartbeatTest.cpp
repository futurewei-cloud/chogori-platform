/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

class HeartbeatTest {

public:  // application lifespan
    HeartbeatTest() : _client(K23SIClientConfig()) {}

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
            .then([this] { return runScenario(); })
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
    K2TxnHandle _txn;
    dto::SKVRecord _record;
    TimePoint _start;

    K23SIClient _client;

public: // tests


// This queues up writes for a specified duration. It is designed to test the heartbeat
// mechanism in the presence of heavy load. It should be used in --poll-mode
seastar::future<> runScenario() {
    K2TxnOptions options{};
    options.syncFinalize = true;
    return _client.beginTxn(options)
    .then([this] (K2TxnHandle&& txn) {
        _txn = std::move(txn);
    })
    .then([this] () {
        return _client.getSchema(collname, "schema", 1)
        .then([this] (auto&& response) {
            auto& [status, schemaPtr] = response;
            K2EXPECT(log::k23si, status.is2xxOK(), true);

            _record = dto::SKVRecord(collname, schemaPtr);
            _record.serializeNext<String>("partkey");
            _record.serializeNext<String>("rangekey");
            _record.serializeNext<String>("data1");
            _record.serializeNext<String>("data2");
        });
    })
    .then([this] () {
        Deadline<> deadline(50ms);

        K2LOG_I(log::k23si, "start time: {}", Clock::now());
        return seastar::do_until(
            [this, d=std::move(deadline)] () { return d.isOver(); },
            [this] () {

                _writeFuture = _writeFuture.then([this] () {
                    return _txn.write<dto::SKVRecord>(_record);
                })
                .then([this](auto&& response) {
                    if (!response.status.is2xxOK()) {
                        K2LOG_W(log::k23si, "failed write: {}", response.status);
                    }
                    K2EXPECT(log::k23si, response.status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                });
                return seastar::make_ready_future<>();
        });
    })
    .then([this] () {
        _start = k2::Clock::now();

        return std::move(_writeFuture);
    })
    .then([this] () {
        auto end = k2::Clock::now();
        auto dur = end - _start;
        K2LOG_I(log::k23si, "Flush time: {}", dur);

        return _txn.end(true);
    })
    .then([](auto&& response) {

        K2EXPECT(log::k23si, response.status, dto::K23SIStatus::OK);
        return seastar::make_ready_future<>();
    });
}

};  // class HeartbeatTest

int main(int argc, char** argv) {
    App app("HeartbeatTest");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("partition_request_timeout", bpo::value<ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<TSO_ClientLib>();
    app.addApplet<HeartbeatTest>();
    return app.start(argc, argv);
}
