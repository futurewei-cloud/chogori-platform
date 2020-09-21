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

#include "schema.h"
#include "datagen.h"
#include "dataload.h"
#include "transactions.h"
#include "verify.h"

using namespace k2;

std::atomic<uint32_t> cores_finished = 0;

std::vector<String> getRangeEnds(uint32_t numPartitions, uint32_t numWarehouses) {
    uint32_t share = numWarehouses / numPartitions;
    share = share == 0 ? 1 : share;
    std::vector<String> rangeEnds;

    // Warehouse IDs start at 1, and range end is open interval
    for (uint32_t i = 1; i <= numPartitions; ++i) {
        String range_end = FieldToKeyString<uint32_t>((i*share)+1);
        K2DEBUG("RangeEnd: " << range_end);
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
        K2INFO("ctor");
    };

    ~Client() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        if (_stopped) {
            return seastar::make_ready_future<>();
        }
        _stopped = true;
        // unregister all observers
        k2::RPC().registerLowTransportMemoryObserver(nullptr);

        return _stopPromise.get_future();
    }

    void registerMetrics() {
        _metric_groups.clear();
        std::vector<sm::label_instance> labels;
        labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

        _metric_groups.add_group("TPC-C", {
            sm::make_counter("completed_txns", _completedTxns, sm::description("Number of completed TPC-C transactions"), labels),
            sm::make_counter("new_order_txns", _newOrderTxns, sm::description("Number of completed New Order transactions"), labels),
            sm::make_counter("payment_txns", _paymentTxns, sm::description("Number of completed Payment transactions"), labels),
            sm::make_histogram("new_order_latency", [this]{ return _newOrderLatency.getHistogram();},
                    sm::description("Latency of New Order transactions"), labels),
            sm::make_histogram("payment_latency", [this]{ return _paymentLatency.getHistogram();},
                    sm::description("Latency of Payment transactions"), labels)

        });
    }

    seastar::future<> start() {
        _stopped = false;

        setupSchemaPointers();

        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: " << ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        registerMetrics();

        return _client.start().then([this] () { return _benchmark(); })
        .handle_exception([this](auto exc) {
            K2ERROR_EXC("Unable to execute benchmark", exc);
            _stopped = true;
            return seastar::make_ready_future<>();
        }).finally([this]() {
            K2INFO("Done with benchmark");
            _stopped = true;
            _stopPromise.set_value();
            cores_finished++;
            if (cores_finished == seastar::smp::count) {
                if (_do_verification()) {
                    K2INFO("Starting verification");
                    return do_with(AtomicVerify(_random, _client, _max_warehouses()),
                                    [] (AtomicVerify& verify) {
                        return verify.run();
                    }).then([this] () {
                        return do_with(ConsistencyVerify(_random, _client, _max_warehouses()),
                                       [] (ConsistencyVerify& verify) {
                           return verify.run().then([] () {
                               K2INFO("Verify done, exiting");
                               ::_exit(0);
                           });
                        });
                    });
                } else {
                    ::_exit(0);
                }
            }

            return make_ready_future<>();
        });
    }

private:
    seastar::future<> _schema_load() {
        std::vector<seastar::future<>> schema_futures;

        schema_futures.push_back(_client.createSchema(tpccCollectionName, Warehouse::warehouse_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, District::district_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, Customer::customer_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, History::history_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, Order::order_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, NewOrder::neworder_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, OrderLine::orderline_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, Item::item_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        schema_futures.push_back(_client.createSchema(tpccCollectionName, Stock::stock_schema)
        .then([] (Status status) {
            K2ASSERT(status.is2xxOK(), "Failed to create schema");
        }));

        return seastar::when_all_succeed(schema_futures.begin(), schema_futures.end());
    }

    seastar::future<> _data_load() {
        K2INFO("Creating DataLoader");
        int cpus = seastar::smp::count;
        int id = seastar::engine().cpu_id();
        int share = _max_warehouses() / cpus;
        if (_max_warehouses() % cpus != 0) {
            K2WARN("CPUs must divide evenly into num warehouses!");
            return make_ready_future<>();
        }

        auto f = seastar::sleep(5s);
        if (id == 0) {
            f = f.then ([this] {
                K2INFO("Creating collection");
                return _client.makeCollection("TPCC", getRangeEnds(_tcpRemotes().size(), _max_warehouses()));
            }).discard_result()
            .then([this] () {
                return _schema_load();
            })
            .then([this] {
                K2INFO("Starting item data load");
                _item_loader = DataLoader(TPCCDataGen().generateItemData());
                return _item_loader.loadData(_client, _num_concurrent_txns());
            });
        } else {
            f = f.then([] { return seastar::sleep(5s); });
        }

        return f.then ([this, share, id] {
            K2INFO("Starting data gen");
            _loader = DataLoader(TPCCDataGen().generateWarehouseData(1+(id*share), 1+(id*share)+share));
            K2INFO("Starting load to server");
            return _loader.loadData(_client, _num_concurrent_txns());
        }).then ([this] {
            K2INFO("Data load done");
        });
    }

    seastar::future<> _tpcc() {
        return seastar::do_until(
            [this] { return _stopped; },
            [this] {
                uint32_t txn_type = _random.UniformRandom(1, 100);
                uint32_t w_id = (seastar::engine().cpu_id() % _max_warehouses()) + 1;
                TPCCTxn* curTxn = txn_type <= 43 ? (TPCCTxn*) new PaymentT(_random, _client, w_id, _max_warehouses())
                                                 : (TPCCTxn*) new NewOrderT(_random, _client, w_id, _max_warehouses());
                auto txn_start = k2::Clock::now();
                return curTxn->run()
                .then([this, txn_type, txn_start] (bool success) {
                    if (!success) {
                        return;
                    }

                    _completedTxns++;
                    auto end = k2::Clock::now();
                    auto dur = end - txn_start;

                    if (txn_type <= 43) {
                        _paymentTxns++;
                        _paymentLatency.add(dur);
                    } else {
                        _newOrderTxns++;
                        _newOrderLatency.add(dur);
                    }
                })
                .finally([curTxn] () {
                    delete curTxn;
                });
            }
        );
    }

    seastar::future<> _benchmark() {
        K2INFO("Creating K23SIClient");

        if (_do_data_load()) {
            return _data_load();
        }

        return seastar::sleep(5s)
        .then([this] {
            K2INFO("Starting transactions...");

            _timer.arm(_testDuration);
            _start = k2::Clock::now();
            _random = RandomContext(seastar::engine().cpu_id());
            for (int i=0; i < _num_concurrent_txns(); ++i) {
                _tpcc_futures.emplace_back(_tpcc());
            }
            return when_all(_tpcc_futures.begin(), _tpcc_futures.end());
        })
        .discard_result()
        .finally([this] () {
            auto duration = k2::Clock::now() - _start;
            auto totalsecs = ((double)k2::msec(duration).count())/1000.0;
            auto cntpsec = (double)_completedTxns/totalsecs;
            auto readpsec = (double)_client.read_ops/totalsecs;
            auto writepsec = (double)_client.write_ops/totalsecs;
            K2INFO("completedTxns=" << _completedTxns << "(" << cntpsec << " per sec)" );
            K2INFO("read ops " << readpsec << " per sec)" );
            K2INFO("write ops " << writepsec << " per sec)" );
            return make_ready_future();
        });
    }

private:
    K23SIClient _client;
    k2::Duration _testDuration;
    bool _stopped;
    seastar::promise<> _stopPromise;
    DataLoader _loader;
    DataLoader _item_loader;
    RandomContext _random;
    k2::TimePoint _start;
    seastar::timer<> _timer;
    std::vector<future<>> _tpcc_futures;

    ConfigVar<std::vector<String>> _tcpRemotes{"tcp_remotes"};
    ConfigVar<bool> _do_data_load{"data_load"};
    ConfigVar<bool> _do_verification{"do_verification"};
    ConfigVar<int> _max_warehouses{"num_warehouses"};
    ConfigVar<int> _num_concurrent_txns{"num_concurrent_txns"};

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _newOrderLatency;
    k2::ExponentialHistogram _paymentLatency;
    uint64_t _completedTxns{0};
    uint64_t _newOrderTxns{0};
    uint64_t _paymentTxns{0};
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
        ("num_warehouses", bpo::value<int>()->default_value(2), "Number of TPC-C Warehouses.")
        ("num_concurrent_txns", bpo::value<int>()->default_value(2), "Number of concurrent transactions to use")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run")
        ("partition_request_timeout", bpo::value<ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("dataload_txn_timeout", bpo::value<ParseableDuration>(), "Timeout of dataload txn, as chrono literal")
        ("writes_per_load_txn", bpo::value<size_t>()->default_value(10), "The number of writes to do in the load phase between txn commit calls")
        ("districts_per_warehouse", bpo::value<uint16_t>()->default_value(10), "The number of districts per warehouse")
        ("customers_per_district", bpo::value<uint32_t>()->default_value(3000), "The number of customers per district")
        ("do_verification", bpo::value<bool>()->default_value(true), "Run verification tests after run")
        ("cpo_request_timeout", bpo::value<ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<ParseableDuration>(), "CPO request backoff");

    app.addApplet<k2::TSO_ClientLib>(0s);
    app.addApplet<Client>();
    return app.start(argc, argv);
}
