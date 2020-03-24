//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <chrono>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>
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
            sm::make_counter("total_count", _totalCount, sm::description("Total number of TPC-C transactions"), labels),
            sm::make_histogram("request_latency", [this]{ return _requestLatency.getHistogram();},
                    sm::description("Latency of TPC-C transactions"), labels)
        });
    }

    seastar::future<> start() {
        _stopped = false;
        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: " << ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        return _benchmark()
        .handle_exception([this](auto exc) {
            K2ERROR_EXC("Unable to execute benchmark", exc);
            _stopped = true;
            return seastar::make_ready_future<>();
        }).finally([this]() {
            K2INFO("Done with benchmark");
            _stopped = true;
            _stopPromise.set_value();
        });
    }

private:
    seastar::future<> _benchmark() {
        K2INFO("Creating K23SIClient");
        _client = K23SIClient(K23SIClientConfig(), _tcpRemotes(), _cpo());
        K2INFO("Creating DataLoader");
        _loader = DataLoader(generateWarehouseData(1, 3));

        return seastar::sleep(1s)
        .then ([this] {
            K2INFO("Creating collection");
            return _client.makeCollection("TPCC");
        }).discard_result()
        .then ([this] {
            K2INFO("Starting load to server");
            return _loader.loadData(_client, 32);
        }).then([this] {
            K2INFO("Warehouse data load done, starting item data load");
            _loader = DataLoader(generateItemData());
            return _loader.loadData(_client, 32);
        }).then([this] {
            K2INFO("Starting transactins...");

            _timer.arm(_testDuration);
            _start = k2::Clock::now();
            _random = RandomContext(0);

            return seastar::do_until(
                [this] { return _stopped; },
                [this] {
                    uint32_t txn_type = _random.UniformRandom(1, 100);
                    TPCCTxn* curTxn = txn_type <= 43 ? (TPCCTxn*) new PaymentT(_random, _client, 1, 2)
                                                     : (TPCCTxn*) new NewOrderT(_random, _client, 1, 2);
                    return curTxn->run()
                    .then([this] (bool success) {
                        if (success) {
                            _totalCount++;
                        }
                    })
                    .finally([this, curTxn] () {
                        delete curTxn; 
                    });
                }
            );
        })
        .finally([this] () {
            auto duration = k2::Clock::now() - _start;
            auto totalsecs = ((double)k2::msec(duration).count())/1000.0;
            auto cntpsec = (double)_totalCount/totalsecs;
            auto readpsec = (double)_client.read_ops/totalsecs;
            auto writepsec = (double)_client.write_ops/totalsecs;
            K2INFO("totalCount=" << _totalCount << "(" << cntpsec << " per sec)" );
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
    RandomContext _random;
    k2::TimePoint _start;
    seastar::timer<> _timer;
    ConfigVar<std::vector<std::string>> _tcpRemotes{"tcp_remotes"};
    ConfigVar<std::string> _cpo{"cpo"};

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _requestLatency;
    std::vector<k2::TimePoint> _requestIssueTimes;
    uint32_t _totalCount{0};
    uint64_t _readOps{0};
    uint64_t _writeOps{0};
}; // class Client

int main(int argc, char** argv) {;
    k2::App app;
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<std::string>>()->multitoken()->default_value(std::vector<std::string>()), "A list(space-delimited) of TCP remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<std::string>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run")
        ("partition_request_timeout", bpo::value<ParseableDuration>(), "Timeout of K23SI operations, as chrono literals");
    app.addApplet<Client>();
    return app.start(argc, argv);
}
