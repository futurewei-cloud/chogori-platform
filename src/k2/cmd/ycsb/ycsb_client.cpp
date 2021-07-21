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

// stl
#include <atomic>
#include <chrono>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/FieldTypes.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>
#include <k2/tso/client/tso_clientlib.h>
#include <k2/common/Timer.h>
#include <seastar/core/sleep.hh>

#include <k2/cmd/ycsb/Log.h>
#include <k2/cmd/ycsb/data.h>
#include <k2/cmd/ycsb/dataload.h>

using namespace k2;

// variable to keep track of number of cores that complete benchmark
std::atomic<uint32_t> cores_finished = 0;

/* This function is used for obtaining the range of YCSB Data keys that will be stored each partition.
   It is invoked while creating YCSB Data collection
   "numPartitions" is the total number of partitions
   "numRows" records is the max number of YCSB Data records that will be stored
   "lenfield" is the max length of each field in a YCSB Data record */
std::vector<String> getRangeEnds(uint32_t numPartitions, size_t numRows, uint32_t lenField) {

    std::deque<String> keys;

    for(size_t i = 0; i < numRows+1; ++i){ // generate all keys for YCSB Data to get Range
        keys.push_back(YCSBData::idToKey(i,lenField));
    }

    std::sort(keys.begin(),keys.end());

    // share is the number of records that will be stored per partition
    // the last partition can have more than share records
    uint32_t share = numRows / numPartitions;
    share = share == 0 ? 1 : share;
    std::vector<String> rangeEnds;

    // range end is open interval
    for (uint32_t i = 1; i <= numPartitions; ++i) {
        String range_end = FieldToKeyString<String>(keys[(i*share)]);
        K2LOG_D(log::ycsb, "RangeEnd: {}", range_end);
        rangeEnds.push_back(range_end);
    }

    // range end of last partition is set to "" as range end is open interval and "" will not be counted as key
    rangeEnds[numPartitions-1] = "";

    return rangeEnds;
}

class Client {
public:  // application lifespan
    Client():
        _client(K23SIClient(K23SIClientConfig())),
        _testDuration(_test_duration_sec()*1s),
        _stopped(true),
        _timer([this] {
            _stopped = true;
        }) {
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

    /* Each transaction will comprise of a set of read, scan, insert, update and delete operations.
       The number of operations per transaction is set as per the specified config.
       For the metrics we measure the number of transcations and the number of each operation */
    void registerMetrics() {
        _metric_groups.clear();
        std::vector<sm::label_instance> labels;
        labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

        _metric_groups.add_group("YCSB", {
            sm::make_counter("completed_txns", _completedTxns, sm::description("Number of completed YCSB transactions"), labels),
            sm::make_counter("read_ops", _readOps, sm::description("Number of completed Read operations"), labels),
            sm::make_counter("scan_ops", _scanOps, sm::description("Number of completed Scan operations"), labels),
            sm::make_counter("insert_ops", _insertOps, sm::description("Number of completed Insert operations"), labels),
            sm::make_counter("update_ops", _updateOps, sm::description("Number of completed Update operations"), labels),
            sm::make_counter("delete_ops", _deleteOps, sm::description("Number of completed Delete operations"), labels),
            sm::make_histogram("txn_latency", [this]{ return _txnLatency.getHistogram();},
                    sm::description("Latency of YCSB transactions"), labels)
        });
    }

    seastar::future<> start() {
        _stopped = false;

        YCSBData::ycsb_schema = YCSBData::generateSchema(_num_fields());
        setupSchemaPointers();

        registerMetrics();

        _benchFuture = _client.start().then([this] () { return _benchmark(); })
        .handle_exception([this](auto exc) {
            K2LOG_W_EXC(log::ycsb, exc, "Unable to execute benchmark");
            _stopped = true;
            return seastar::make_ready_future<>();
        }).finally([this]() {
            K2LOG_I(log::ycsb, "Done with benchmark");
            cores_finished++;
            if (cores_finished == seastar::smp::count) {
                if (_do_verification()) {
                    // to implement verification later
                } else {
                    seastar::engine().exit(0);
                }
            }

            return seastar::make_ready_future<>();
        });

        return seastar::make_ready_future<>();
    }

private:
    // This function is used for loading the YCSB Data Schema
    seastar::future<> _schemaLoad() {

        return _client.createSchema(ycsbCollectionName, YCSBData::ycsb_schema)
        .then([] (auto&& result) {
            K2ASSERT(log::ycsb, result.status.is2xxOK(), "Failed to create schema");
        });
    }

    /* This function is used for loading data.
       It first creates a collection for YCSB data and then loads the data */
    seastar::future<> _dataLoad() {
        K2LOG_I(log::ycsb, "Creating DataLoader");
        int cpus = seastar::smp::count;
        int id = seastar::this_shard_id();
        int share = _num_records() / cpus; // number of records loaded per cpu, the last one can have more than share loads

        auto f = seastar::sleep(5s); // sleep for collection to be loaded first
        if (id == 0) { // only shard 0 loads the collection
            f = f.then ([this] {
                K2LOG_I(log::ycsb, "Creating collection");
                return _client.makeCollection("YCSB", getRangeEnds(_tcpRemotes().size(), _num_records(),_field_length()));
            }).discard_result()
            .then([this] () {
                return _schemaLoad();
            });
        } else {
            f = f.then([] { return seastar::sleep(5s); });
        }

        return f.then ([this, share, id, cpus] {
            K2LOG_I(log::ycsb, "Starting data gen and load in shard {}", id);
            _data_loader = DataLoader();
            size_t end_idx = (id*share)+share; // end_idx to load record
            if(id==(cpus-1))
                end_idx = _num_records();
            return _data_loader.loadData(_client, _num_concurrent_txns(),(id*share),end_idx); // load records from id*share to end_idx (not inclusive)
        }).then ([this] {
            K2LOG_I(log::ycsb, "Data load done");
        });
    }

    /* This function will perform data loading if do_data_load is set
       and perform YCSB Benchmarking for given workload

       TODO - Yet to implement Benchmarking part !!! */
    seastar::future<> _benchmark() {

        K2LOG_I(log::ycsb, "Creating K23SIClient");

        if (_do_data_load()) {
            return _dataLoad();
        }

        return seastar::make_ready_future();
    }

private:
    K23SIClient _client;
    k2::Duration _testDuration;
    bool _stopped = true;
    DataLoader _data_loader;
    RandomContext _random;
    k2::TimePoint _start;
    SingleTimer _timer;
    std::vector<seastar::future<>> _ycsb_futures;
    seastar::future<> _benchFuture = seastar::make_ready_future<>();

    ConfigVar<std::vector<String>> _tcpRemotes{"tcp_remotes"};
    ConfigVar<bool> _do_data_load{"data_load"};
    ConfigVar<bool> _do_verification{"do_verification"};
    ConfigVar<int> _num_concurrent_txns{"num_concurrent_txns"};
    ConfigVar<uint32_t> _num_fields{"num_fields"};
    ConfigVar<uint32_t> _field_length{"field_length"};
    ConfigVar<size_t> _num_records{"num_records"};
    ConfigVar<uint32_t> _test_duration_sec{"test_duration_s"};

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _txnLatency;
    uint64_t _completedTxns{0};
    uint64_t _readOps{0};
    uint64_t _insertOps{0};
    uint64_t _updateOps{0};
    uint64_t _scanOps{0};
    uint64_t _deleteOps{0};
}; // class Client

int main(int argc, char** argv) {;
    k2::App app("YCSBClient");
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
