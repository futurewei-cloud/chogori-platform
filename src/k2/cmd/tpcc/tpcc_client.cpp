//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <chrono>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/RetryStrategy.h>
#include <seastar/core/sleep.hh>

#include "mock/mock_k23si_client.h"
#include "schema.h"
#include "datagen.h"
#include "dataload.h"
#include "transactions.h"

using namespace k2;

class Client {
public:  // application lifespan
    Client():
        _client(K23SIClient(K23SIClientConfig())),
        _tcpRemotes(k2::Config()["tcp_remotes"].as<std::vector<std::string>>()),
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
        labels.push_back(sm::label_instance("active_cores", std::min(_tcpRemotes.size(), size_t(seastar::smp::count))));

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
        return _discovery().then([this](){
            if (_stopped) return seastar::make_ready_future<>();
            K2INFO("Setup complete. Starting benchmark...");
            return _benchmark();
        }).
        handle_exception([this](auto exc) {
            K2ERROR("Unable to execute benchmark. " << exc);
            _stopped = true;
            return seastar::make_ready_future<>();
        }).finally([this]() {
            K2INFO("Done with benchmark");
            _stopped = true;
            _stopPromise.set_value();
        });
    }

private:
    seastar::future<> _discovery() {
        auto myID = seastar::engine().cpu_id();
        K2INFO("performing service discovery on core " << myID);
        if (myID >= _tcpRemotes.size()) {
            K2WARN("No TCP remote endpoint defined for core " << myID);
            return seastar::make_exception_future<>(std::runtime_error("No remote endpoint defined"));
        }
        auto myRemote = k2::RPC().getTXEndpoint(_tcpRemotes[myID]);
        auto retryStrategy = seastar::make_lw_shared<k2::ExponentialBackoffStrategy>();
        retryStrategy->withRetries(10).withStartTimeout(1s).withRate(5);
        return retryStrategy->run([this, myRemote=std::move(myRemote)](size_t retriesLeft, k2::Duration timeout) {
            K2INFO("Sending with retriesLeft=" << retriesLeft << ", and timeout="
                    << k2::msec(timeout).count()
                    << "ms, with " << myRemote->getURL());
            if (_stopped) {
                K2INFO("Stopping retry since we were stopped");
                return seastar::make_exception_future<>(std::runtime_error("we were stopped"));
            }
            return k2::RPC().sendRequest(k2::MessageVerbs::GET_DATA_URL, myRemote->newPayload(), *myRemote, timeout)
            .then([this](std::unique_ptr<k2::Payload> payload) {
                if (_stopped) return seastar::make_ready_future<>();
                if (!payload || payload->getSize() == 0) {
                    K2ERROR("Remote end did not provide a data endpoint. Giving up");
                    return seastar::make_exception_future<>(std::runtime_error("no remote endpoint"));
                }
                k2::String remoteURL;
                for (auto&& buf: payload->release()) {
                    remoteURL.append((char*)buf.get_write(), buf.size());
                }
                K2INFO("Found remote data endpoint: " << remoteURL);
                _remoteEndpoint = *(k2::RPC().getTXEndpoint(remoteURL));
                return seastar::make_ready_future<>();
            })
            .then_wrapped([this](auto&& fut) {
                if (_stopped) {
                    fut.ignore_ready_future();
                    return seastar::make_ready_future<>();
                }
                return std::move(fut);
            });
        })
        .finally([retryStrategy](){
            K2INFO("Finished getting remote data endpoint");
        });
    }

    seastar::future<> _benchmark() {
        _client._remote_endpoint = _remoteEndpoint;
        _loader = DataLoader(generateWarehouseData(1, 3));

        return seastar::sleep(1s)
        .then ([this] {
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
                    _totalCount++;
                    uint32_t txn_type = _random.UniformRandom(1, 100);
                    TPCCTxn* curTxn = txn_type <= 43 ? (TPCCTxn*) new PaymentT(_random, _client, 1, 2)
                                                     : (TPCCTxn*) new NewOrderT(_random, _client, 1, 2);
                    return curTxn->run().finally([curTxn] () { delete curTxn; });
                }
            );
        })
        .finally([this] () {            
            auto duration = k2::Clock::now() - _start;
            auto totalsecs = ((double)k2::msec(duration).count())/1000.0;
            auto cntpsec = (double)_totalCount/totalsecs;
            K2INFO("totalCount=" << _totalCount << "(" << cntpsec << " per sec)" );
            return make_ready_future();
        });
    }

private:
    K23SIClient _client;
    std::vector<std::string> _tcpRemotes;
    k2::TXEndpoint _remoteEndpoint;
    k2::Duration _testDuration;
    bool _stopped;
    seastar::promise<> _stopPromise;
    DataLoader _loader;
    RandomContext _random;
    k2::TimePoint _start;
    seastar::timer<> _timer;

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _requestLatency;
    std::vector<k2::TimePoint> _requestIssueTimes;
    uint32_t _totalCount = 0;
}; // class Client

int main(int argc, char** argv) {;
    k2::App app;
    app.addApplet<Client>();
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<std::string>>()->multitoken()->default_value(std::vector<std::string>()), "A list(space-delimited) of TCP remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run");
    return app.start(argc, argv);
}
