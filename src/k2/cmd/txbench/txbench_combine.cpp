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
#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/RetryStrategy.h>

#include "txbench_common.h"
#include <cstdlib>
#include <ctime>
#include <unordered_map>

class Service : public seastar::weakly_referencable<Service> {
public:  // application lifespan
    Service():
        _stopped(true) {
        K2INFO("ctor");
    };

    virtual ~Service() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        _stopped = true;
        // unregistar all observers
        k2::RPC().registerMessageObserver(GET_DATA_URL, nullptr);
        k2::RPC().registerMessageObserver(REQUEST, nullptr);
        k2::RPC().registerMessageObserver(START_SESSION, nullptr);
        k2::RPC().registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        _stopped = false;

        _registerDATA_URL();
        _registerSTART_SESSION();
        _registerREQUEST();

        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        return seastar::make_ready_future<>();
    }

private:
    void _registerDATA_URL() {
        K2INFO("TCP endpoint is: " << k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto)->getURL());
        k2::RPC().registerMessageObserver(GET_DATA_URL,
            [this](k2::Request&& request) mutable {
                auto response = request.endpoint.newPayload();
                auto ep = (seastar::engine()._rdma_stack?
                           k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto):
                           k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto));
                K2INFO("GET_DATA_URL responding with data endpoint: " << ep->getURL());
                response->write(ep->getURL());
                return k2::RPC().sendReply(std::move(response), request);
            });
    }

    void _registerSTART_SESSION() {
        k2::RPC().registerMessageObserver(START_SESSION, [this](k2::Request&& request) mutable {
            auto sid = uint64_t(std::rand());
            if (request.payload) {
                SessionConfig config{};
                request.payload->read((void*)&config, sizeof(config));

                BenchSession session(request.endpoint, sid, config);
                auto result = _sessions.try_emplace(sid, std::move(session));
                assert(result.second);
                K2INFO("Starting new session: " << sid);
                auto resp = request.endpoint.newPayload();
                SessionAck ack{.sessionID=sid};
                resp->write((void*)&ack, sizeof(ack));
                return k2::RPC().sendReply(std::move(resp), request);
            }
            return seastar::make_ready_future();
        });
    }

    void _registerREQUEST() {
        k2::RPC().registerMessageObserver(REQUEST, [this](k2::Request&& request) mutable {
            if (request.payload) {
                uint64_t sid = 0;
                uint64_t reqId = 0;
                request.payload->read((void*)&sid, sizeof(sid));
                request.payload->read((void*)&reqId, sizeof(reqId));

                auto siditer = _sessions.find(sid);
                if (siditer != _sessions.end()) {
                    auto& session = siditer->second;
                    session.runningSum += reqId;
                    session.totalSize += session.config.responseSize;
                    session.totalCount += 1;
                    session.unackedSize += session.config.responseSize;
                    session.unackedCount += 1;
                    if (session.config.echoMode) {
                        auto&& buffs = request.payload->release();
                        _data.insert(_data.end(), std::move_iterator(buffs.begin()), std::move_iterator(buffs.end()));
                    }
                    if (session.unackedCount >= session.config.ackCount) {
                        auto response = request.endpoint.newPayload();
                        Ack ack{.sessionID=sid, .totalCount=session.totalCount, .totalSize=session.totalSize, .checksum=session.runningSum};
                        response->write((void*)&ack, sizeof(ack));
                        if (session.config.echoMode) {
                            for (auto&& buf: _data) {
                                response->write(buf.get(), buf.size());
                            }
                            _data.clear();
                        }
                        return k2::RPC().send(ACK, std::move(response), request.endpoint).
                        then([&] (){
                            session.unackedSize = 0;
                            session.unackedCount = 0;
                        });
                        
                    }
                }
            }
            return seastar::make_ready_future();
        });
    }

private:
    // flag we need to tell if we've been stopped
    bool _stopped;
    std::unordered_map<uint64_t, BenchSession> _sessions;
    std::vector<k2::Binary> _data;
}; // class Service

class Client {
public:  // application lifespan
    Client():
        _tcpRemotes(k2::Config()["tcp_remotes"].as<std::vector<k2::String>>()),
        _testDuration(k2::Config()["test_duration_s"].as<uint32_t>()*1ms),
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
    seastar::future<> gracefulStop() {
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
            K2ERROR_EXC("Unable to execute benchmark", exc);
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
            uint64_t vector_size = _duration_counts.size();
            if (vector_size > 0){
                K2INFO("Latency 50% =" << _duration_counts[(int)(vector_size/100.0*50)]  << "ns");
                K2INFO("Latency 90% =" <<  _duration_counts[(int)(vector_size/100.0*90)] << "ns");
                K2INFO("Latency 99% ==" <<  _duration_counts[(int)(vector_size/100.0*99)] << "ns");
                K2INFO("Latency 99.9% ==" << _duration_counts[(int)(vector_size/100.0*99.9)]  << "ns");
                K2INFO("Latency 99.99% ==" << _duration_counts[(int)(vector_size/100.0*99.99)]  << "ns");

                K2INFO("Latency Size: " << vector_size);
                for (uint64_t i=10;i>=1;--i){
                    K2INFO("Latency " << i << " ==" << _duration_counts[vector_size-i]  << "ns");
                }
            }

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
            .then([this](std::unique_ptr<k2::Payload>&& payload) {
                if (_stopped) return seastar::make_ready_future<>();
                if (!payload || payload->getSize() == 0) {
                    K2ERROR("Remote end did not provide a data endpoint. Giving up");
                    return seastar::make_exception_future<>(std::runtime_error("no remote endpoint"));
                }
                k2::String remoteURL;
                if (!payload->read(remoteURL)) {
                    K2ERROR("Unable to read remote URL in GET_DATA_URL response");
                    return seastar::make_exception_future<>(std::runtime_error("cannot parse remote endpoint"));
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
        req->write((void*)&_session.config, sizeof(_session.config));
        return k2::RPC().sendRequest(START_SESSION, std::move(req), _session.client, 1s).
        then([this](std::unique_ptr<k2::Payload>&& payload) {
            if (_stopped) return seastar::make_ready_future<>();
            if (!payload || payload->getSize() == 0) {
                K2ERROR("Remote end did not start a session. Giving up");
                return seastar::make_exception_future<>(std::runtime_error("no remote session"));
            }
            SessionAck ack{};
            payload->read((void*)&ack, sizeof(ack));
            _session.sessionID = ack.sessionID;
            K2INFO("Starting session id=" << _session.sessionID);
            return seastar::make_ready_future<>();
        }).
        handle_exception([](auto exc) {
            K2ERROR_EXC("Unable to start session", exc);
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
                request.payload->read((void*)&ack, sizeof(ack));
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
                    _duration_counts.push_back(k2::nsec(dur).count());
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
                    return send();
                }
                else {
                    assert(!_haveSendPromise);
                    _haveSendPromise = true;
                    _sendProm = seastar::promise<>();
                    return _sendProm.get_future();
                }
            }
        ).finally([this] {
            int vector_size = _duration_counts.size();
            K2INFO("Total Size: " << vector_size);
            uint64_t max = 0;
            for (int i = 0; i<vector_size;++i){
                if (_duration_counts[i] >= 10000000){
                    K2INFO("Large Size: " << i<<" " << _duration_counts[i]);
                }
                if (_duration_counts[i] > max)
                    max = _duration_counts[i];
            }
            K2INFO("Maximum Size: " << max);
            
            std::sort(_duration_counts.begin(), _duration_counts.end()); 
            K2INFO("Total Size2: " << _duration_counts.size());
            _actualTestDuration = k2::Clock::now() - _start;

        });
    }

    bool canSend() {
        //K2DEBUG("can send: uasize=" << _session.unackedSize <<", ppsize=" << _session.config.pipelineSize
        //        <<", uacount=" <<  _session.unackedCount <<", ppcount=" << _session.config.pipelineCount);
        return _session.unackedSize < _session.config.pipelineSize &&
               _session.unackedCount < _session.config.pipelineCount;
    }

    seastar::future<> send() {
        auto payload = _session.client.newPayload();
        auto padding = sizeof(_session.sessionID) + sizeof(_session.totalCount);
        assert(padding < _session.config.responseSize);
        _session.totalSize += _session.config.responseSize;
        _session.totalCount += 1;
        _session.unackedSize += _session.config.responseSize;
        _session.unackedCount += 1;

        payload->write((void*)&_session.sessionID, sizeof(_session.sessionID));
        payload->write((void*)&_session.totalCount, sizeof(_session.totalCount));
        payload->write(_data, _session.config.responseSize - padding );
        _requestIssueTimes[_session.totalCount % _session.config.pipelineCount] = k2::Clock::now();
        return k2::RPC().send(REQUEST, std::move(payload), _session.client);
    }

private:
    std::vector<k2::String> _tcpRemotes;
    std::vector<uint64_t> _duration_counts;
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
    k2::App app("txbench_client");
    app.addApplet<Service>();
    app.addApplet<Client>();
    app.addOptions()
        ("request_size", bpo::value<uint32_t>()->default_value(512), "How many bytes to send with each request")
        ("tx_xcore_loopback", bpo::value<bool>()->default_value(false), "Whether enable the in process loopback across different cores")
        ("ack_count", bpo::value<uint32_t>()->default_value(5), "How many messages do we ack at once")
        ("pipeline_depth_mbytes", bpo::value<uint32_t>()->default_value(200), "How much data do we allow to go un-ACK-ed")
        ("pipeline_depth_count", bpo::value<uint32_t>()->default_value(10), "How many requests do we allow to go un-ACK-ed")
        ("echo_mode", bpo::value<bool>()->default_value(false), "Should we echo all data in requests when we ACK. ")
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of TCP remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("test_duration_s", bpo::value<uint32_t>()->default_value(30), "How long in seconds to run");
    return app.start(argc, argv);
}