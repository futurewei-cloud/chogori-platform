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
#include <seastar/core/sleep.hh>

#include "rpcbench_common.h"
#include "Log.h"
using namespace k2;

class Client {
public:  // application lifespan
    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::txbench, "stopping");
        _stopped = true;
        return std::move(_benchFut);
    }

    void registerMetrics() {
        _metric_groups.clear();
        std::vector<sm::label_instance> labels;
        labels.push_back(sm::label_instance("total_cores", seastar::smp::count));
        labels.push_back(sm::label_instance("active_cores", std::min(_remotes().size(), size_t(seastar::smp::count))));
        _metric_groups.add_group("session",
        {
            sm::make_gauge("session_id", _session.sessionId, sm::description("Session ID"), labels),
            sm::make_counter("total_count", _session.totalCount, sm::description("Total number of requests"), labels),
            sm::make_counter("total_bytes", _session.totalSize, sm::description("Total data bytes sent"), labels),
            sm::make_histogram("request_latency", [this]{ return _requestLatency.getHistogram();}, sm::description("Latency of acks"), labels)
        });
    }

    seastar::future<> start() {
        _stopped = false;
        _session = std::move(BenchSession(0, _requestSize()));
        auto myid = seastar::engine().cpu_id();

        // push all eps to talk to, starting with mine
        for (size_t i = myid; i < _multiConn() + myid; ++i) {
            _session.endpoints.push_back(k2::RPC().getTXEndpoint(_remotes()[i%_remotes().size()]));
        }
        K2LOG_I(log::txbench, "Setup complete. Starting session...");
        _benchFut = _benchFut
        .then([this]{
            return _startSession();
        })
        .then([this]() {
            K2LOG_I(log::txbench, "Starting benchmark in session: {}", _session.sessionId);
            return _benchmark();
        })
        .handle_exception([this](auto exc) {
            K2LOG_W_EXC(log::txbench, exc, "Unable to execute benchmark");
            return seastar::make_ready_future<>();
        })
        .finally([this]() {
            K2LOG_I(log::txbench, "Done with benchmark");
        });

        return seastar::make_ready_future();
    }

private:

    seastar::future<> _startSession() {
        registerMetrics();
        TXBenchStartSession startReq{.responseSize=_responseSize()};
        std::vector<seastar::future<>> futs;
        for (auto& ep: _session.endpoints) {
            futs.push_back(
                k2::RPC().callRPC<TXBenchStartSession, TXBenchStartSessionAck>(MsgVerbs::START_SESSION, startReq, *ep, _multiConn()*1s)
                .then([this](auto&& reply) mutable {
                    auto& [status, resp] = reply;
                    if (!status.is2xxOK()) {
                        K2LOG_E(log::txbench, "Cannot start session: {}", status);
                        return seastar::make_exception_future(std::runtime_error("server cannot start session"));
                    }
                    K2LOG_I(log::txbench, "started session {}", resp.sessionId);
                    _session.sessionId = resp.sessionId;
                    return seastar::make_ready_future();
                })
            );
        }

        return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result();
    }

    seastar::future<> _benchmark() {
        K2LOG_I(log::txbench, "Starting benchmark for main remote={}, with requestSize={}, with responseSize={}, with pipelineDepth={}, with multiConn={}, with copyData={}, with testDuration={}",
            _session.endpoints[0]->getURL(), _requestSize(), _responseSize(), _pipelineDepth(),
             _multiConn(), _copyData(), _testDuration());

        std::vector<seastar::future<>> reqFuts;
        reqFuts.push_back(seastar::sleep(_testDuration()).then([this]{_stopped = true;}));
        for (size_t i = 0; i < _pipelineDepth(); ++i) {
            for (size_t j = 0; j < _multiConn(); ++j) {
                reqFuts.push_back(_runRequest(*_session.endpoints[j]));
            }
        }
        return seastar::when_all_succeed(reqFuts.begin(), reqFuts.end());
    }

    seastar::future<> _runRequest(k2::TXEndpoint& ep) {
        return seastar::do_until(
            [this] { return _stopped; },
            [this, &ep] {
                if (_copyData()) {
                    TXBenchRequest<k2::String> req;
                    req.data.val = _session.dataCopy;
                    req.sessionId = _session.sessionId;
                    auto started = k2::Clock::now();
                    return k2::RPC().callRPC<TXBenchRequest<k2::String>, TXBenchResponse<k2::Payload>>(MsgVerbs::REQUEST_COPY, req, ep, 1s)
                    .then([this, started] (auto&&) {
                        _session.totalCount ++;
                        _session.totalSize += _requestSize() + _responseSize();
                        _requestLatency.add(k2::Clock::now() - started);
                    });
                }
                else {
                    TXBenchRequest<k2::Payload> req;
                    req.data.val = _session.dataShare.shareAll();
                    req.sessionId = _session.sessionId;
                    auto started = k2::Clock::now();
                    return k2::RPC().callRPC<TXBenchRequest<k2::Payload>, TXBenchResponse<k2::Payload>>(MsgVerbs::REQUEST, req, ep, 1s)
                    .then([this, started] (auto&&) {
                        _session.totalCount ++;
                        _session.totalSize += _requestSize() + _responseSize();
                        _requestLatency.add(k2::Clock::now() - started);
                    });

                }
            });
    }

private:
    k2::ConfigVar<std::vector<k2::String>> _remotes{"remote_eps"};
    k2::ConfigDuration _testDuration{"test_duration", 30s};
    k2::ConfigVar<uint32_t> _requestSize{"request_size"};
    k2::ConfigVar<uint32_t> _responseSize{"response_size"};
    k2::ConfigVar<uint32_t> _pipelineDepth{"pipeline_depth"};
    k2::ConfigVar<bool> _copyData{"copy_data"};
    k2::ConfigVar<uint32_t> _multiConn{"multi_conn"};
    BenchSession _session;
    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _requestLatency;
    seastar::future<> _benchFut = seastar::make_ready_future();
    bool _stopped = true;
}; // class Client

int main(int argc, char** argv) {
    k2::App app("RPCBenchClient");
    app.addApplet<Client>();
    app.addOptions()
        ("request_size", bpo::value<uint32_t>()->default_value(512), "How many bytes to send with each request")
        ("copy_data", bpo::value<bool>()->default_value(false), "Copy instead of share the data when sending requests")
        ("multi_conn", bpo::value<uint32_t>()->default_value(1), "how many conns to use per core (each with the pipeline_depth below)")
        ("response_size", bpo::value<uint32_t>()->default_value(512), "How many bytes to receive with each response")
        ("pipeline_depth", bpo::value<uint32_t>()->default_value(10), "How many requests to have in the pipeline")
        ("remote_eps", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("test_duration", bpo::value<k2::ParseableDuration>(), "How long to run");
    return app.start(argc, argv);
}
