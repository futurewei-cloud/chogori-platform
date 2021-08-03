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


#include <optional>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/infrastructure/APIServer.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/tso/client/tso_clientlib.h>

#include <seastar/core/sleep.hh>

#include "Log.h"

const char* collname="HTTPClient";
k2::dto::Schema _schema {
    .name = "test_schema",
    .version = 1,
    .fields = std::vector<k2::dto::SchemaField> {
     {k2::dto::FieldType::STRING, "partitionKey", false, false},
     {k2::dto::FieldType::STRING, "rangeKey", false, false},
     {k2::dto::FieldType::STRING, "data", false, false}
    },
    .partitionKeyFields = std::vector<uint32_t> { 0 },
    .rangeKeyFields = std::vector<uint32_t> { 1 },
};
static thread_local std::shared_ptr<k2::dto::Schema> schemaPtr;

class ReadRequest{
public:
};


class Client {
public:  // application lifespan
    Client():
        _client(k2::K23SIClientConfig()) {
    }
    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        _stopped = true;
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        _stopped = false;
        _registerAPI();
        auto myid = seastar::this_shard_id();
        schemaPtr = std::make_shared<k2::dto::Schema>(_schema);
        auto _startFut = seastar::make_ready_future<>();
        _startFut = _startFut.then([this] {return _client.start();});
        if (myid == 0) {
            K2LOG_I(k2::log::httpclient, "Creating collection...");
            _startFut = _startFut.then([this] {
                return _client.makeCollection(collname).discard_result()
                .then([this] () {
                    K2LOG_I(k2::log::httpclient, "Creating schema...");
                    return _client.createSchema(collname, _schema);
                }).discard_result();
            });
        }

        return _startFut;
    }

private:
    seastar::future<nlohmann::json> _handleBegin(nlohmann::json&& request) {
        (void) request;
        return _client.beginTxn(k2::K2TxnOptions())
        .then([this] (auto&& txn) {
            K2LOG_D(k2::log::httpclient, "begin txn: {}", txn.mtr());
            _txns[_txnID++] = std::move(txn);
            nlohmann::json response;
            response["status"] = 200;
            response["txnID"] = _txnID - 1;
            return seastar::make_ready_future<nlohmann::json>(std::move(response));
        });
    }

    seastar::future<nlohmann::json> _handleEnd(nlohmann::json&& request) {
        return seastar::make_ready_future<nlohmann::json>(std::move(request));
    }
    seastar::future<nlohmann::json> _handleRead(nlohmann::json&& request) {
        return seastar::make_ready_future<nlohmann::json>(std::move(request));
    }
    seastar::future<nlohmann::json> _handleWrite(nlohmann::json&& request) {
        return seastar::make_ready_future<nlohmann::json>(std::move(request));
    }

    void _registerAPI() {
        K2LOG_I(k2::log::httpclient, "Registering HTTP API observers...");
        k2::APIServer& api_server = k2::AppBase().getDist<k2::APIServer>().local();

        api_server.registerRawAPIObserver("BeginTxn", "Begin a txn, returning a numeric txn handle", [this](nlohmann::json&& request) {
            return _handleBegin(std::move(request));
        });
    }

    void _registerMetrics() {
        _metric_groups.clear();
        std::vector<sm::label_instance> labels;
        labels.push_back(sm::label_instance("total_cores", seastar::smp::count));
        labels.push_back(sm::label_instance("active_cores", size_t(seastar::smp::count)));
        _metric_groups.add_group("session",
        {
            sm::make_counter("total_txns", _totalTxns, sm::description("Total number of transactions"), labels),
            sm::make_counter("aborted_txns", _abortedTxns, sm::description("Total number of aborted transactions"), labels),
            sm::make_counter("committed_txns", _committedTxns, sm::description("Total number of committed transactions"), labels),
            sm::make_counter("total_reads", _totalReads, sm::description("Total number of reads"), labels),
            sm::make_counter("success_reads", _successReads, sm::description("Total number of successful reads"), labels),
            sm::make_counter("fail_reads", _failReads, sm::description("Total number of failed reads"), labels),
            sm::make_counter("total_writes", _totalWrites, sm::description("Total number of writes"), labels),
            sm::make_counter("success_writes", _successWrites, sm::description("Total number of successful writes"), labels),
            sm::make_counter("fail_writes", _failWrites, sm::description("Total number of failed writes"), labels),
            sm::make_histogram("read_latency", [this]{ return _readLatency.getHistogram();}, sm::description("Latency of reads"), labels),
            sm::make_histogram("write_latency", [this]{ return _writeLatency.getHistogram();}, sm::description("Latency of writes"), labels),
            sm::make_histogram("txn_latency", [this]{ return _txnLatency.getHistogram();}, sm::description("Latency of entire txns"), labels),
            sm::make_histogram("txnend_latency", [this]{ return _endLatency.getHistogram();}, sm::description("Latency of txn end request"), labels)
        });
    }

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _readLatency;
    k2::ExponentialHistogram _writeLatency;
    k2::ExponentialHistogram _txnLatency;
    k2::ExponentialHistogram _endLatency;

    uint64_t _totalTxns=0;
    uint64_t _abortedTxns=0;
    uint64_t _committedTxns=0;
    uint64_t _totalReads = 0;
    uint64_t _successReads = 0;
    uint64_t _failReads = 0;
    uint64_t _totalWrites = 0;
    uint64_t _successWrites = 0;
    uint64_t _failWrites = 0;

    bool _stopped = true;
    k2::K23SIClient _client;
    uint64_t _txnID = 0;
    std::unordered_map<uint64_t, k2::K2TxnHandle> _txns;
};  // class Client

int main(int argc, char** argv) {
    k2::App app("K23SIBenchClient");
    app.addApplet<k2::APIServer>();
    app.addApplet<k2::TSO_ClientLib>();
    app.addApplet<Client>();
    app.addOptions()
        // config for dependencies
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff");
    return app.start(argc, argv);
}
