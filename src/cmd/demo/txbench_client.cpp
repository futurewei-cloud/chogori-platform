//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <exception>
#include <chrono>
#include <cctype> // for is_print
#include <string>
#include <cstdlib>
#include <ctime>

// third-party
#include <seastar/core/distributed.hh> // for distributed<>
#include <seastar/core/app-template.hh> // for app_template
#include <seastar/core/reactor.hh> // for app_template
#include <seastar/util/reference_wrapper.hh> // for app_template
#include <seastar/core/future.hh> // for future stuff
#include <seastar/core/timer.hh> // periodic timer
#include <seastar/core/metrics_registration.hh> // metrics
#include <seastar/core/metrics.hh>

using namespace std::chrono_literals; // so that we can type "1ms"
namespace bpo = boost::program_options;
namespace sm = seastar::metrics;

// k2 transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/RRDMARPCProtocol.h"
#include "common/Log.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"
#include "transport/Prometheus.h"

#include "txbench_common.h"

class Client {
public: // public types
    // distributed version of the class
    typedef seastar::distributed<Client> Dist_t;

public:  // application lifespan
    Client(k2::RPCDispatcher::Dist_t& dispatcher, const bpo::variables_map& config):
        _disp(dispatcher.local()),
        _tcpRemotes(config["tcp_remotes"].as<std::vector<std::string>>()),
        _testDuration(config["test_duration_s"].as<uint32_t>()*1s),
        _data(0),
        _stopped(true),
        _haveSendPromise(false),
        _timer(seastar::timer<>([this] {
            _stopped = true;
            if (_haveSendPromise) {
                _haveSendPromise = false;
                _sendProm.set_value();
            }
        })),
        _lastAckedTotal(0) {
        _session.config = SessionConfig{.echoMode=config["echo_mode"].as<bool>(),
                                        .responseSize=config["request_size"].as<uint32_t>(),
                                        .pipelineSize=config["pipeline_depth_mbytes"].as<uint32_t>() * 1024 * 1024,
                                        .pipelineCount=config["pipeline_depth_count"].as<uint32_t>(),
                                        .ackCount=config["ack_count"].as<uint32_t>()};
        _data = (char*)malloc(_session.config.responseSize);
        K2INFO("ctor");
    };

    ~Client() {
        K2INFO("dtor");
        free(_data);
    }

    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2INFO("stop");
        if (_stopped) {
            return seastar::make_ready_future<>();
        }
        _stopped = true;
        // unregistar all observers
        _disp.registerMessageObserver(ACK, nullptr);
        _disp.registerLowTransportMemoryObserver(nullptr);
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
    }

    seastar::future<> start() {
        _stopped = false;
        _disp.registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        return _discovery().then([this](){
            if (_stopped) return seastar::make_ready_future<>();
            K2INFO("Setup complete. Starting session...");
            return _startSession();
        }).
        then([this]() {
            if (_stopped) return seastar::make_ready_future<>();
            K2INFO("Setup complete. Starting benchmark in session: " << _session.sessionID);
            return _benchmark();
        }).
        handle_exception([this](auto exc) {
            K2ERROR("Unable to execute benchmark. " << exc);
            _stopped = true;
            return seastar::make_ready_future<>();
        }).finally([this]() {
            K2INFO("Done with benchmark");
            auto totalsecs = ((double)std::chrono::duration_cast<std::chrono::milliseconds>(_actualTestDuration).count())/1000.0;
            auto szpsec = (((double)_session.totalSize - _session.unackedSize )/(1024*1024*1024))/totalsecs;
            auto cntpsec = ((double)_session.totalCount - _session.unackedCount)/totalsecs;
            K2INFO("sessionID=" << _session.sessionID);
            K2INFO("remote=" << _session.client.getURL());
            K2INFO("totalSize=" << _session.totalSize << " (" << szpsec*8 << " GBit per sec)");
            K2INFO("totalCount=" << _session.totalCount << "(" << cntpsec << " per sec)" );
            K2INFO("unackedSize=" << _session.unackedSize);
            K2INFO("unackedCount=" << _session.unackedCount);
            K2INFO("testDuration=" << std::chrono::duration_cast<std::chrono::milliseconds>(_actualTestDuration).count()  << "ms");
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
        auto myRemote = _disp.getTXEndpoint(_tcpRemotes[myID]);
        auto retryStrategy = seastar::make_lw_shared<k2::ExponentialBackoffStrategy>();
        retryStrategy->withRetries(10).withStartTimeout(1s).withRate(5);
        return retryStrategy->run([this, myRemote=std::move(myRemote)](size_t retriesLeft, k2::Duration timeout) {
            K2INFO("Sending with retriesLeft=" << retriesLeft << ", and timeout="
                    << std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count()
                    << "ms, with " << myRemote->getURL());
            if (_stopped) {
                K2INFO("Stopping retry since we were stopped");
                return seastar::make_exception_future<>(std::runtime_error("we were stopped"));
            }
            return _disp.sendRequest(GET_DATA_URL, myRemote->newPayload(), *myRemote, timeout)
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
                _session.client = *(_disp.getTXEndpoint(remoteURL));
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

    seastar::future<> _startSession() {
        _requestIssueTimes.clear();
        _requestIssueTimes.resize(_session.config.pipelineCount);
        _lastAckedTotal = 0;
        registerMetrics();

        auto req = _session.client.newPayload();
        req->getWriter().write((void*)&_session.config, sizeof(_session.config));
        return _disp.sendRequest(START_SESSION, std::move(req), _session.client, 1s).
        then([this](std::unique_ptr<k2::Payload> payload) {
            if (_stopped) return seastar::make_ready_future<>();
            if (!payload || payload->getSize() == 0) {
                K2ERROR("Remote end did not start a session. Giving up");
                return seastar::make_exception_future<>(std::runtime_error("no remote session"));
            }
            SessionAck ack{};
            payload->getReader().read((void*)&ack, sizeof(ack));
            _session.sessionID = ack.sessionID;
            K2INFO("Starting session id=" << _session.sessionID);
            return seastar::make_ready_future<>();
        }).
        handle_exception([](auto exc) {
            K2ERROR("Unable to start session: " << exc);
            return seastar::make_exception_future<>(exc);
        });
    }

    seastar::future<> _benchmark() {
        K2INFO("Starting benchmark for remote=" << _session.client.getURL() <<
             ", with requestSize=" << _session.config.responseSize <<
             ", with pipelineDepthBytes=" << _session.config.pipelineSize <<
             ", with pipelineDepthCount=" << _session.config.pipelineCount <<
             ", with ackCount=" << _session.config.ackCount <<
             ", with echoMode=" << _session.config.echoMode <<
             ", with testDuration=" << std::chrono::duration_cast<std::chrono::milliseconds>(_testDuration).count()  << "ms");

        _disp.registerMessageObserver(ACK, [this](k2::Request&& request) {
            auto now = k2::Clock::now(); // to compute reqest latencies
            if (request.payload) {
                Ack ack{};
                auto rdr = request.payload->getReader();
                rdr.read((void*)&ack, sizeof(ack));
                if (ack.sessionID != _session.sessionID) {
                    K2WARN("Received ack for unknown session: have=" << _session.sessionID
                           << ", recv=" << ack.sessionID);
                    return;
                }
                if (ack.totalCount > _session.totalCount) {
                    K2WARN("Received ack for too many messages: have=" << _session.totalCount
                           << ", recv=" << ack.totalCount);
                    return;
                }
                if (ack.totalCount <= _lastAckedTotal) {
                    K2WARN("Received ack that is too old tc=" << _session.totalCount << ", uc=" << _session.unackedCount << ", ac=" << ack.totalCount);
                }
                if (ack.totalSize > _session.totalSize) {
                    K2WARN("Received ack for too much data: have=" << _session.totalSize
                           << ", recv=" << ack.totalSize);
                    return;
                }
                if (ack.checksum != (ack.totalCount*(ack.totalCount+1)) /2) {
                    K2WARN("Checksum mismatch. got=" << ack.checksum << ", expected=" << (ack.totalCount*(ack.totalCount+1)) /2);
                }
                for (uint64_t reqid = _session.totalCount - _session.unackedCount; reqid < ack.totalCount; reqid++) {
                    auto idx = reqid % _session.config.pipelineCount;
                    auto dur = now - _requestIssueTimes[idx];
                    _requestLatency.add(dur);
                }
                _session.unackedCount = _session.totalCount - ack.totalCount;
                _session.unackedSize = _session.totalSize - ack.totalSize;
                if (_haveSendPromise) {
                    _haveSendPromise = false;
                    _sendProm.set_value();
                }
            }
        });

        _timer.arm(_testDuration);

        _start = k2::Clock::now();
        return seastar::do_until(
            [this] { return _stopped; },
            [this] { // body of loop
                if (canSend()) {
                    send();
                }
                else {
                    assert(!_haveSendPromise);
                    _haveSendPromise = true;
                    _sendProm = seastar::promise<>();
                    return _sendProm.get_future();
                }
                return seastar::make_ready_future<>();
            }
        ).finally([this] {
            _actualTestDuration = k2::Clock::now() - _start;
        });
    }

    bool canSend() {
        //K2DEBUG("can send: uasize=" << _session.unackedSize <<", ppsize=" << _session.config.pipelineSize
        //        <<", uacount=" <<  _session.unackedCount <<", ppcount=" << _session.config.pipelineCount);
        return _session.unackedSize < _session.config.pipelineSize &&
               _session.unackedCount < _session.config.pipelineCount;
    }

    void send() {
        auto payload = _session.client.newPayload();
        auto padding = sizeof(_session.sessionID) + sizeof(_session.totalCount);
        assert(padding < _session.config.responseSize);
        _session.totalSize += _session.config.responseSize;
        _session.totalCount += 1;
        _session.unackedSize += _session.config.responseSize;
        _session.unackedCount += 1;

        payload->getWriter().write((void*)&_session.sessionID, sizeof(_session.sessionID));
        payload->getWriter().write((void*)&_session.totalCount, sizeof(_session.totalCount));
        payload->getWriter().write(_data, _session.config.responseSize - padding );
        _requestIssueTimes[_session.totalCount % _session.config.pipelineCount] = k2::Clock::now();
        _disp.send(REQUEST, std::move(payload), _session.client);
    }

private:
    k2::RPCDispatcher& _disp;
    std::vector<std::string> _tcpRemotes;
    k2::Duration _testDuration;
    k2::Duration _actualTestDuration;
    char* _data;
    // flag we need to tell if we've been stopped
    bool _stopped;
    BenchSession _session;
    seastar::promise<> _sendProm;
    seastar::promise<> _stopPromise;
    bool _haveSendPromise;
    k2::TimePoint _start;
    seastar::timer<> _timer;
    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _requestLatency;
    std::vector<k2::TimePoint> _requestIssueTimes;
    uint64_t _lastAckedTotal;
}; // class Client

int main(int argc, char** argv) {
    k2::VirtualNetworkStack::Dist_t vnet;
    k2::RPCProtocolFactory::Dist_t tcpproto;
    k2::RPCProtocolFactory::Dist_t rrdmaproto;
    k2::RPCDispatcher::Dist_t dispatcher;
    Client::Dist_t client;
    k2::Prometheus prometheus;

    seastar::app_template app;
    app.add_options()
        ("tcp_remotes", bpo::value<std::vector<std::string>>()->multitoken(), "The remote tcp endpoints")
        ("prometheus_port", bpo::value<uint16_t>()->default_value(8089), "HTTP port for the prometheus server")
        ("request_size", bpo::value<uint32_t>()->default_value(512), "How many bytes to send with each request")
        ("ack_count", bpo::value<uint32_t>()->default_value(5), "How many messages do we ack at once")
        ("pipeline_depth_mbytes", bpo::value<uint32_t>()->default_value(200), "How much data do we allow to go un-ACK-ed")
        ("pipeline_depth_count", bpo::value<uint32_t>()->default_value(10), "How many requests do we allow to go un-ACK-ed")
        ("echo_mode", bpo::value<bool>()->default_value(false), "Should we echo all data in requests when we ACK. ")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run");

    // we are now ready to assemble the running application
    auto result = app.run_deprecated(argc, argv, [&] {
        auto& config = app.configuration();
        uint16_t promport = config["prometheus_port"].as<uint16_t>();

        // call the stop() method on each object when we're about to exit. This also deletes the objects
        seastar::engine().at_exit([&] {
            K2INFO("prometheus stop");
            return prometheus.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("vnet stop");
            return vnet.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("tcpproto stop");
            return tcpproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("rrdma stop");
            return rrdmaproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("dispatcher stop");
            return dispatcher.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("client stop");
            return client.stop();
        });

        return
            // OBJECT CREATION (via distributed<>.start())
            [&]{
                K2INFO("Start prometheus");
                return prometheus.start(promport, "K2 txbench client metrics", "txbench_client");
            }()
            .then([&] {
                K2INFO("create vnet");
                return vnet.start();
            })
            .then([&]() {
                K2INFO("create tcpproto");
                return tcpproto.start(k2::TCPRPCProtocol::builder(std::ref(vnet)));
            })
            .then([&]() {
                K2INFO("Client started... create RDMA");
                return rrdmaproto.start(k2::RRDMARPCProtocol::builder(std::ref(vnet)));
            })
            .then([&]() {
                K2INFO("create dispatcher");
                return dispatcher.start();
            })
            .then([&]() {
                K2INFO("create client");
                return client.start(std::ref(dispatcher), app.configuration());
            })
            // STARTUP LOGIC
            .then([&]() {
                K2INFO("Start VNS");
                return vnet.invoke_on_all(&k2::VirtualNetworkStack::start);
            })
            .then([&]() {
                K2INFO("Start tcpproto");
                return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
            })
            .then([&]() {
                K2INFO("Register TCP Protocol");
                // Could register more protocols here via separate invoke_on_all calls
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
            })
            .then([&]() {
                K2INFO("Start RDMA");
                return rrdmaproto.invoke_on_all(&k2::RPCProtocolFactory::start);
            })
            .then([&]() {
                K2INFO("Register RDMA Protocol");
                // Could register more protocols here via separate invoke_on_all calls
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(rrdmaproto));
            })
            .then([&]() {
                K2INFO("Start dispatcher");
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
            })
            .then([&]() {
                K2INFO("client start");
                return client.invoke_on_all(&Client::start);
            }).then([]{
                K2INFO("******* TEST COMPLETE *******");
                K2INFO("Hit ctrl+c to terminate...");
            });
    });
    K2INFO("Shutdown was successful!");
    return result;
}
