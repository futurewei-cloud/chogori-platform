//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <chrono>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>
#include <k2/tso/client_lib/tso_clientlib.h>
#include <seastar/core/sleep.hh>

#include "schema.h"
#include "datagen.h"
#include "dataload.h"
#include "transactions.h"

using namespace k2;

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
    seastar::future<> stop() {
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
            seastar::engine().exit(0);
        });
    }

private:
    seastar::future<> _data_load() {
        K2INFO("Creating DataLoader");
        int cpus = seastar::smp::count;
        int id = seastar::engine().cpu_id();
        int share = _max_warehouses() / cpus;
        if (_max_warehouses() % cpus != 0) {
            K2WARN("CPUs must divide envenly into num warehouses!");
            return make_ready_future<>();
        }

        _loader = DataLoader(generateWarehouseData(1+(id*share), 1+(id*share)+share));

        auto f = seastar::sleep(1s);
        if (id == 0) {
            f = f.then ([this] {
                K2INFO("Creating collection");
                return _client.makeCollection("TPCC");
            }).discard_result()
            .then([this] {
                K2INFO("Starting item data load");
                _item_loader = DataLoader(generateItemData());
                return _item_loader.loadData(_client, _num_concurrent_txns());
            });
        } else {
            f = f.then([] { return seastar::sleep(2s); });
        }

        return f.then ([this] {
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
                uint32_t w_id = _random.UniformRandom(1, _max_warehouses());
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

        return seastar::sleep(1s)
        .then([this] {
            K2INFO("Starting transactins...");

            _timer.arm(_testDuration);
            _start = k2::Clock::now();
            _random = RandomContext(seastar::engine().cpu_id());
            for (int i=0; i<_clients_per_core(); ++i) {
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

    ConfigVar<bool> _do_data_load{"data_load"};
    ConfigVar<int> _max_warehouses{"num_warehouses"};
    ConfigVar<int> _clients_per_core{"clients_per_core"};
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
    k2::App app;
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of TCP remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("data_load", bpo::value<bool>()->default_value(false), "If true, only data gen and load are performed. If false, only benchmark is performed.")
        ("num_warehouses", bpo::value<int>()->default_value(2), "Number of TPC-C Warehouses.")
        ("num_concurrent_txns", bpo::value<int>()->default_value(2), "Number of concurrent transactions to use")
        ("clients_per_core", bpo::value<int>()->default_value(1), "Number of concurrent TPC-C clients per core")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run")
        ("partition_request_timeout", bpo::value<ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("dataload_txn_timeout", bpo::value<ParseableDuration>(), "Timeout of dataload txn, as chrono literal")
        ("cpo_request_timeout", bpo::value<ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<ParseableDuration>(), "CPO request backoff");

    app.addApplet<k2::TSO_ClientLib>(0s);
    app.addApplet<Client>();
    return app.start(argc, argv);
}
