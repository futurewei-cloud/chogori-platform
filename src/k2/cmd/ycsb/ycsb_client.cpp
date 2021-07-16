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

// stl
#include <atomic>
#include <chrono>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/FieldTypes.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>
#include <k2/tso/client/tso_clientlib.h>
#include <seastar/core/sleep.hh>

#include "Log.h"
#include "data.h"
#include "dataload.h"

using namespace k2;

std::atomic<uint32_t> cores_finished = 0;

std::vector<String> getRangeEnds(uint32_t numPartitions, size_t numRows, uint32_t len_field) {


    std::vector<String> keys;

    for(size_t i = 0; i < numRows+1; ++i){ // generate all keys for YCSB Data to getRange for collection
        keys.push_back(YCSBData::idToKey(i,len_field));
    }

    std::sort(keys.begin(),keys.end());

    uint32_t share = numRows / numPartitions;
    share = share == 0 ? 1 : share;
    std::vector<String> rangeEnds;

    // range end is open interval
    for (uint32_t i = 1; i <= numPartitions; ++i) {
        String range_end = FieldToKeyString<String>(keys[(i*share)]);
        K2LOG_D(log::ycsb, "RangeEnd: {}", range_end);
        rangeEnds.push_back(range_end);
    }

    rangeEnds[numPartitions-1] = "";

    return rangeEnds;
}

class Client {
public:  // application lifespan
    Client():
        _client(K23SIClient(K23SIClientConfig())),
        _testDuration(k2::Config()["test_duration_s"].as<uint32_t>()*1s),
        _stopped(true),
        _timer(seastar::timer<>([this] {
            _stopped = true;
        })) {
        K2LOG_I(log::ycsb, "ctor");
    };

    ~Client() {
        K2LOG_I(log::ycsb, "dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::ycsb, "stop");
        _stopped = true;
        // unregister all observers
        k2::RPC().registerLowTransportMemoryObserver(nullptr);

        return std::move(_benchFuture);
    }

    void registerMetrics() {
        _metric_groups.clear();
        std::vector<sm::label_instance> labels;
        labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

        _metric_groups.add_group("YCSB", {
            sm::make_counter("completed_txns", _completedTxns, sm::description("Number of completed YCSB transactions"), labels),
            sm::make_counter("read_txns", _readTxns, sm::description("Number of completed Read transactions"), labels),
            sm::make_counter("scan_txns", _scanTxns, sm::description("Number of completed Scan transactions"), labels),
            sm::make_counter("insert_txns", _insertTxns, sm::description("Number of completed Insert transactions"), labels),
            sm::make_counter("update_txns", _updateTxns, sm::description("Number of completed Update transactions"), labels),
            sm::make_counter("delete_txns", _deleteTxns, sm::description("Number of completed Delete transactions"), labels),
            sm::make_histogram("read_latency", [this]{ return _readLatency.getHistogram();},
                    sm::description("Latency of Read transactions"), labels),
            sm::make_histogram("scan_latency", [this]{ return _scanLatency.getHistogram();},
                    sm::description("Latency of Scan transactions"), labels),
            sm::make_histogram("insert_latency", [this]{ return _insertLatency.getHistogram();},
                    sm::description("Latency of Insert transactions"), labels),
            sm::make_histogram("update_latency", [this]{ return _updateLatency.getHistogram();},
                    sm::description("Latency of Update transactions"), labels),
            sm::make_histogram("delete_latency", [this]{ return _deleteLatency.getHistogram();},
                    sm::description("Latency of Delete transactions"), labels)
        });
    }

    seastar::future<> start() {
        _stopped = false;

        YCSBData::ycsb_schema = YCSBData::generate_schema(_num_fields());
        setupSchemaPointers();

        registerMetrics();

        _benchFuture = _client.start().then([this] () { return _benchmark(); })
        .handle_exception([this](auto exc) {
            K2LOG_W_EXC(log::ycsb, exc, "Unable to execute benchmark");
            _stopped = true;
            return seastar::make_ready_future<>();
        }).finally([this]() {
            K2LOG_I(log::ycsb, "Done with benchmark");
            _stopped = true;
            cores_finished++;
            if (cores_finished == seastar::smp::count) {
                if (_do_verification()) {
                    // to implement verification later
                } else {
                    seastar::engine().exit(0);
                }
            }

            return make_ready_future<>();
        });

        return make_ready_future<>();
    }

private:
    seastar::future<> _schema_load() {
        std::vector<seastar::future<>> schema_futures;

        schema_futures.push_back(_client.createSchema(ycsbCollectionName, YCSBData::ycsb_schema)
        .then([] (auto&& result) {
            K2ASSERT(log::ycsb, result.status.is2xxOK(), "Failed to create schema");
        }));

        return seastar::when_all_succeed(schema_futures.begin(), schema_futures.end());
    }

    seastar::future<> _data_load() {
        K2LOG_I(log::ycsb, "Creating DataLoader");

        auto f = seastar::sleep(5s);

        K2LOG_D(log::ycsb, "smp count: {}",seastar::smp::count);

        return f.then ([this] {
            K2LOG_I(log::ycsb, "Creating collection");
            return _client.makeCollection("YCSB", getRangeEnds(_tcpRemotes().size(), _num_records(),_field_length()));
        }).discard_result()
        .then([this] () {
            return _schema_load();
        })
        .then([this] {
            K2LOG_I(log::ycsb, "Starting data gen and load");
            _data_loader = DataLoader();
            return _data_loader.loadData(_client, _num_concurrent_txns());
        }).then ([this] {
            K2LOG_I(log::ycsb, "Data load done");
        });
    }

    seastar::future<> _benchmark() {

        K2LOG_I(log::ycsb, "Creating K23SIClient");

        if (_do_data_load()) {
            return _data_load();
        }

        return make_ready_future();
    }

private:
    K23SIClient _client;
    k2::Duration _testDuration;
    bool _stopped = true;
    DataLoader _data_loader;
    RandomContext _random;
    k2::TimePoint _start;
    seastar::timer<> _timer;
    std::vector<future<>> _ycsb_futures;
    seastar::future<> _benchFuture = seastar::make_ready_future<>();

    ConfigVar<std::vector<String> > _tcpRemotes{"tcp_remotes"};
    ConfigVar<bool> _do_data_load{"data_load"};
    ConfigVar<bool> _do_verification{"do_verification"};
    ConfigVar<int> _num_concurrent_txns{"num_concurrent_txns"};
    ConfigVar<uint32_t> _num_fields{"num_fields"};
    ConfigVar<uint32_t> _field_length{"field_length"};
    ConfigVar<size_t> _num_records{"num_records"};

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _scanLatency;
    k2::ExponentialHistogram _updateLatency;
    k2::ExponentialHistogram _insertLatency;
    k2::ExponentialHistogram _readLatency;
    k2::ExponentialHistogram _deleteLatency;
    uint64_t _completedTxns{0};
    uint64_t _readTxns{0};
    uint64_t _insertTxns{0};
    uint64_t _updateTxns{0};
    uint64_t _scanTxns{0};
    uint64_t _deleteTxns{0};

    uint64_t _readOps{0};
    uint64_t _writeOps{0};
}; // class Client

int main(int argc, char** argv) {;
    k2::App app("TPCCClient");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of TCP remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("data_load", bpo::value<bool>()->default_value(false), "If true, only data gen and load are performed. If false, only benchmark is performed.")
        ("num_concurrent_txns", bpo::value<int>()->default_value(2), "Number of concurrent transactions to use")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run")
        ("partition_request_timeout", bpo::value<ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("dataload_txn_timeout", bpo::value<ParseableDuration>(), "Timeout of dataload txn, as chrono literal")
        ("writes_per_load_txn", bpo::value<size_t>()->default_value(10), "The number of writes to do in the load phase between txn commit calls")
        ("do_verification", bpo::value<bool>()->default_value(true), "Run verification tests after run")
        ("cpo_request_timeout", bpo::value<ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<ParseableDuration>(), "CPO request backoff")
        ("num_records",bpo::value<size_t>()->default_value(1000),"How many records to load")
        ("field_length",bpo::value<uint32_t>()->default_value(10),"The size of all fields in the table")
        ("num_fields",bpo::value<uint32_t>()->default_value(5), "The number of fields in the table");

    app.addApplet<k2::TSO_ClientLib>();
    app.addApplet<Client>();
    return app.start(argc, argv);
}
