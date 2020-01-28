//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/RetryStrategy.h>
#include <seastar/core/sleep.hh>

#include "mock/mock_k23si_client.h"
#include "tpcc_rand.h"
#include "tpcc_schema.h"

using namespace k2;

class Client {
public:  // application lifespan
    Client():
        _client(K23SIClient(K23SIClientConfig())),
        _tcpRemotes(k2::Config()["tcp_remotes"].as<std::vector<std::string>>()),
        _stopped(true),
        _haveSendPromise(false),
        _timer(seastar::timer<>([this] {
            _stopped = true;
            if (_haveSendPromise) {
                _haveSendPromise = false;
                _sendProm.set_value();
            }
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
        if (_haveSendPromise) {
            _sendProm.set_value();
        }
        return _stopPromise.get_future();
    }

    void registerMetrics() {
        _metric_groups.clear();
        std::vector<sm::label_instance> labels;
        labels.push_back(sm::label_instance("total_cores", seastar::smp::count));
        labels.push_back(sm::label_instance("active_cores", std::min(_tcpRemotes.size(), size_t(seastar::smp::count))));
/*
        _metric_groups.add_group("session", {
            sm::make_gauge("ack_batch_size", _session.config.ackCount, sm::description("How many messages we ack at once"), labels),
            sm::make_gauge("session_id", _session.sessionID, sm::description("Session ID"), labels),
            sm::make_counter("total_count", _session.totalCount, sm::description("Total number of requests"), labels),
            sm::make_counter("total_bytes", _session.totalSize, sm::description("Total data bytes sent"), labels),
            sm::make_gauge("pipeline_depth", [this]{ return _session.config.pipelineCount  - _session.unackedCount;},
                    sm::description("Available pipeline depth"), labels),
            sm::make_gauge("pipeline_bytes", [this]{ return _session.config.pipelineSize  - _session.unackedSize;},
                    sm::description("Available pipeline bytes"), labels),
            sm::make_histogram("request_latency", [this]{ return _requestLatency.getHistogram();},
                    sm::description("Latency of acks"), labels)
        });
*/
    }

    seastar::future<> start() {
        _stopped = false;
        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
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
                _remote_endpoint = *(k2::RPC().getTXEndpoint(remoteURL));
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
        _client._remote_endpoint = _remote_endpoint;

        return seastar::sleep(1s)
        .then ([this] {
            K2TxnOptions options;
            return _client.beginTxn(options);
        })
        .then([this] (K2TxnHandle t) {
            return do_with(std::move(t), [this] (K2TxnHandle& txn) {
                RandomContext random(0);
                Warehouse warehouse(random, 1);

                K2INFO("putting record");
                Record request = {};
                request.key.partition_key = warehouse.getPartitionKey();
                request.key.row_key = warehouse.getRowKey();
                //request.value = Payload([] () { return Binary(4096); });
                auto payload = _remote_endpoint.newPayload();
                warehouse.writeData(*payload);
                request.value = payload->share();

                return txn.write(std::move(request))
                .then([this, &txn](auto resp) {
                    K2INFO("Received write response with status: " << resp.status);

                    K2INFO("getting record");
                    Key request = {};
                    request.partition_key = "test1:";
                    request.row_key = "test2";
                    return txn.read(std::move(request));
                })
                .then([this, &txn](ReadResult resp) {
                    K2INFO("Received GET response status=" << resp.status);
                    return txn.end(true);
                });
            });
        })
        .then([] (auto t) {
            (void) t;
            return;
        });
    }

private:
    K23SIClient _client;
    std::vector<std::string> _tcpRemotes;
    k2::TXEndpoint _remote_endpoint;
    bool _stopped;
    seastar::promise<> _sendProm;
    seastar::promise<> _stopPromise;
    bool _haveSendPromise;
    k2::TimePoint _start;
    seastar::timer<> _timer;
    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _requestLatency;
    std::vector<k2::TimePoint> _requestIssueTimes;
}; // class Client

int main(int argc, char** argv) {
    k2::App app;
    app.addApplet<Client>();
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<std::string>>()->multitoken()->default_value(std::vector<std::string>()), "A list(space-delimited) of TCP remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    return app.start(argc, argv);
}
