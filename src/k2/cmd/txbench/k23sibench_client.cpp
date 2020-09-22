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
#include <optional>
#include <random>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/tso/client/tso_clientlib.h>

#include <seastar/core/sleep.hh>

const char* collname="K23SIBench";
k2::dto::Schema _schema {
    .name = "bench_schema",
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

class DataRec{
public:
    std::optional<k2::String> partitionKey;
    std::optional<k2::String> rangeKey;
    std::optional<k2::String> data;

    std::shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = collname;

    SKV_RECORD_FIELDS(partitionKey, rangeKey, data);
};


class KeyGen {
public:
    KeyGen(size_t start) : _start(start), _idx(start) {}

    DataRec next() {
        k2::String stridx = std::to_string(_idx);
        _idx ++;

        DataRec record{
            .partitionKey = "partkey:" + stridx,
            .rangeKey = stridx,
            .data = std::nullopt,
            .schema = schemaPtr
        };

        return record;
    }
    void reset() {
        _idx = _start;
    }
private:
    size_t _start;
    size_t _idx;
};

class Client {
public:  // application lifespan
    Client():
        _client(k2::K23SIClientConfig()) {
        K2INFO("ctor");
    }
    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stopping");
        _stopped = true;
        return std::move(_benchFut);
    }

    seastar::future<> start() {
        K2INFO("Starting benchmark" <<
            ", with dataSize=" << _dataSize() <<
            ", with reads=" << _reads() <<
            ", with writes=" << _writes() <<
            ", with pipelineDepth=" << _pipelineDepth() <<
            ", with testDuration=" << _testDuration());
        _data = k2::String('.', _dataSize());
        _stopped = false;
        auto myid = seastar::engine().cpu_id();
        schemaPtr = std::make_shared<k2::dto::Schema>(_schema);
        _gen.seed(myid);

        _benchFut = seastar::sleep(5s);
        _benchFut = _benchFut.then([this] {return _client.start();});
        if (myid == 0) {
            K2INFO("Creating collection...");
            _benchFut = _benchFut.then([this] {
                return _client.makeCollection(collname).discard_result()
                .then([this] () {
                    return _client.createSchema(collname, _schema);
                }).discard_result();
            });
        } else {
            _benchFut = _benchFut.then([] { return seastar::sleep(5s); });
        }

        _benchFut = _benchFut
        .then([this] {
            registerMetrics();
            std::vector<seastar::future<>> futs;
            futs.push_back(seastar::sleep(_testDuration()).then([this] { _stopped = true; }));
            for (size_t i = 0; i < _pipelineDepth(); ++i) {
                futs.push_back(_startSession());
            }
            return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result();
        })
        .handle_exception([](auto exc) {
            K2ERROR_EXC("Unable to execute benchmark", exc);
            return seastar::make_ready_future();
        })
        .finally([this] {
            K2INFO("Done with benchmark");
        });

        return seastar::make_ready_future();
    }

private:
    seastar::future<> _startSession() {
        return seastar::do_until(
            [this] { return _stopped; },
            [this] {
                k2::K2TxnOptions opts{};
                opts.deadline = k2::Deadline(_txnTimeout());
                opts.syncFinalize = _sync_finalize();
                auto start = k2::Clock::now();
                return _client.beginTxn(opts)
                .then([this, start](k2::K2TxnHandle&& txn) {
                    _totalTxns ++;
                    return seastar::do_with(std::move(txn), [this, start] (auto& txn) {
                        return _runTxn(start, txn);
                    });
                })
                .handle_exception([] (auto exc) {
                    K2ERROR_EXC("Txn failed: ", exc);
                    return seastar::make_ready_future<>();
                });
            });
    }

    seastar::future<> _runTxn(k2::TimePoint start, k2::K2TxnHandle& txn) {
        return seastar::do_with(true, k2::TimePoint{}, KeyGen(_dist(_gen)), [this, &txn, start] (bool& willCommit, k2::TimePoint& endStart, KeyGen& keygen) {
            return seastar::make_ready_future()
            .then([this, &txn, &keygen] {
                if (_stopped) return seastar::make_ready_future();
                // issue writes
                std::vector<seastar::future<>> writes;
                for (size_t i = 0; i < _writes(); ++i) {
                    writes.push_back(_doWrite(txn, keygen));
                }
                return seastar::when_all_succeed(writes.begin(), writes.end()).discard_result();
            })
            .then([this, &txn, &keygen] {
                if (_stopped) return seastar::make_ready_future();
                // issue reads
                keygen.reset();
                std::vector<seastar::future<>> reads;
                for (size_t i = 0; i < _reads(); ++i) {
                    reads.push_back(_doRead(txn, keygen));
                }
                return seastar::when_all_succeed(reads.begin(), reads.end()).discard_result();
            })
            // finalize
            .then_wrapped([this, &txn, &willCommit, start, &endStart] (auto&& fut) {
                if (_stopped) return seastar::make_ready_future<k2::EndResult>(k2::EndResult(k2::dto::K23SIStatus::OperationNotAllowed));
                fut.ignore_ready_future();
                willCommit = !fut.failed();
                endStart = k2::Clock::now();
                return txn.end(willCommit);
            }).then_wrapped([this, &txn, &willCommit, start, &endStart] (auto&& fut) {
                if (_stopped) return seastar::make_ready_future();
                auto now = k2::Clock::now();
                _txnLatency.add(now - start);
                _endLatency.add(now - endStart);
                if (fut.failed()) {
                    K2ERROR_EXC("txn end failed with", fut.get_exception());
                    return seastar::make_ready_future();
                }

                k2::EndResult result = fut.get0();

                if (result.status.is2xxOK()) {
                    if (willCommit) {
                        _committedTxns ++;
                    }
                    else {
                        _abortedTxns ++;
                    }
                }
                else {
                    K2ERROR("Unable to end transaction " << txn << ", due to : " << result.status);
                }
                return seastar::make_ready_future();
            });
        });
    }

    seastar::future<> _doRead(k2::K2TxnHandle& txn, KeyGen& keygen) {
        ++_totalReads;
        return seastar::do_with(k2::Clock::now(), [this, &txn, &keygen] (auto& start) {
            return txn.read<DataRec>(keygen.next())
            .then([this, start](auto&& result) {
                _readLatency.add(k2::Clock::now() - start);
                if (!result.status.is2xxOK() && result.status != k2::dto::K23SIStatus::KeyNotFound) {
                    ++_failReads;
                    K2ERROR("Failed to read key due to: " << result.status);
                    return seastar::make_exception_future(std::runtime_error("failed to read key"));
                }
                ++_successReads;
                return seastar::make_ready_future();
            });
        });
    }

    seastar::future<> _doWrite(k2::K2TxnHandle& txn, KeyGen& keygen) {
        ++_totalWrites;
        return seastar::do_with(k2::Clock::now(), [this, &txn, &keygen](auto& start) {
            DataRec record = keygen.next();
            record.data = _data;
            return txn.write<DataRec>(record)
                .then([this, start](auto&& result) {
                    _writeLatency.add(k2::Clock::now() - start);
                    if (!result.status.is2xxOK()) {
                        ++_failWrites;
                        K2ERROR("Failed to write key due to: " << result.status);
                        return seastar::make_exception_future(std::runtime_error("failed to write key"));
                    }
                    ++_successWrites;
                    return seastar::make_ready_future();
                });
        });
    }

private://metrics
    void registerMetrics() {
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

   private:
    k2::ConfigVar<uint32_t> _dataSize{"data_size"};
    k2::ConfigVar<uint32_t> _reads{"reads"};
    k2::ConfigVar<uint32_t> _writes{"writes"};
    k2::ConfigVar<uint32_t> _pipelineDepth{"pipeline_depth"};
    k2::ConfigVar<bool> _sync_finalize{"sync_finalize"};
    k2::ConfigDuration _testDuration{"test_duration", 30s};
    k2::ConfigDuration _txnTimeout{"txn_timeout", 10s};

    seastar::future<> _benchFut = seastar::make_ready_future();
    bool _stopped = true;
    k2::K23SIClient _client;
    k2::String _data;
    std::mt19937 _gen;
    std::uniform_int_distribution<> _dist;
};  // class Client

int main(int argc, char** argv) {
    k2::App app("K23SIBenchClient");
    app.addApplet<k2::TSO_ClientLib>(0s);
    app.addApplet<Client>();
    app.addOptions()
        ("data_size", bpo::value<uint32_t>()->default_value(512), "How many bytes to write in records")
        ("reads", bpo::value<uint32_t>()->default_value(1), "How many reads to do in each txn")
        ("writes", bpo::value<uint32_t>()->default_value(1), "How many writes to do in each txn")
        ("pipeline_depth", bpo::value<uint32_t>()->default_value(10), "How many transactions to run concurrently")
        ("sync_finalize", bpo::value<bool>()->default_value(false), "K23SI Sync finalize option")
        ("test_duration", bpo::value<k2::ParseableDuration>(), "How long to run")
        ("txn_timeout", bpo::value<k2::ParseableDuration>(), "timeout for each transaction")
        // config for dependencies
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff");
    return app.start(argc, argv);
}
