//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/RetryStrategy.h>

#include "txbench_common.h"

class Client {
public:  // application lifespan
    Client():
        _tcpRemotes(k2::Config()["tcp_remotes"].as<std::vector<std::string>>()),
        _testDuration(k2::Config()["test_duration_s"].as<uint32_t>()*1s),
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
        _session.config = SessionConfig{.echoMode=k2::Config()["echo_mode"].as<bool>(),
                                        .responseSize=k2::Config()["request_size"].as<uint32_t>(),
                                        .pipelineSize=k2::Config()["pipeline_depth_mbytes"].as<uint32_t>() * 1024 * 1024,
                                        .pipelineCount=k2::Config()["pipeline_depth_count"].as<uint32_t>(),
                                        .ackCount=k2::Config()["ack_count"].as<uint32_t>()};
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
        // unregister all observers
        k2::RPC().registerMessageObserver(ACK, nullptr);
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
        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
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
            auto totalsecs = ((double)k2::msec(_actualTestDuration).count())/1000.0;
            auto szpsec = (((double)_session.totalSize - _session.unackedSize )/(1024*1024*1024))/totalsecs;
            auto cntpsec = ((double)_session.totalCount - _session.unackedCount)/totalsecs;
            K2INFO("sessionID=" << _session.sessionID);
            K2INFO("remote=" << _session.client.getURL());
            K2INFO("totalSize=" << _session.totalSize << " (" << szpsec*8 << " GBit per sec)");
            K2INFO("totalCount=" << _session.totalCount << "(" << cntpsec << " per sec)" );
            K2INFO("unackedSize=" << _session.unackedSize);
            K2INFO("unackedCount=" << _session.unackedCount);
            K2INFO("testDuration=" << k2::msec(_actualTestDuration).count()  << "ms");
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
            return k2::RPC().sendRequest(GET_DATA_URL, myRemote->newPayload(), *myRemote, timeout)
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
                _session.client = *(k2::RPC().getTXEndpoint(remoteURL));
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
        return k2::RPC().sendRequest(START_SESSION, std::move(req), _session.client, 1s).
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
             ", with testDuration=" << k2::msec(_testDuration).count()  << "ms");

        k2::RPC().registerMessageObserver(ACK, [this](k2::Request&& request) {
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
        k2::RPC().send(REQUEST, std::move(payload), _session.client);
    }

private:
    std::vector<std::string> _tcpRemotes;
    k2::Duration _testDuration;
    k2::Duration _actualTestDuration;
    char* _data;
    BenchSession _session;
    bool _stopped;
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
    k2::App app;
    app.addApplet<Client>();
    app.addOptions()
        ("request_size", bpo::value<uint32_t>()->default_value(512), "How many bytes to send with each request")
        ("ack_count", bpo::value<uint32_t>()->default_value(5), "How many messages do we ack at once")
        ("pipeline_depth_mbytes", bpo::value<uint32_t>()->default_value(200), "How much data do we allow to go un-ACK-ed")
        ("pipeline_depth_count", bpo::value<uint32_t>()->default_value(10), "How many requests do we allow to go un-ACK-ed")
        ("echo_mode", bpo::value<bool>()->default_value(false), "Should we echo all data in requests when we ACK. ")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run");
    return app.start(argc, argv);
}
