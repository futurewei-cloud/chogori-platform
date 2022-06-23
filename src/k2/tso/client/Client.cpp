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


#include <random>
#include <algorithm>

#include <seastar/core/sleep.hh>

#include <k2/dto/ControlPlaneOracle.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/transport/RetryStrategy.h>

#include "Client.h"

namespace k2::tso {
using namespace dto;

void TSOClient::_registerMetrics() {
    _metricGroups.clear();
    std::vector<sm::label_instance> labels;

    _metricGroups.add_group("TSOClient", {
        sm::make_histogram("get_timestamp_latency", [this]{ return _latency.getHistogram();},
                sm::description("Observed latency for get_timestamp calls"), labels),
        sm::make_histogram("discovery_latency", [this]{ return _discoveryLatency.getHistogram();},
                sm::description("Observed latency for performing worker discovery"), labels)
    });
}

seastar::future<> TSOClient::start() {
    if (_cpoEndpoint() == "") {
        // This is the case for a nodepool process, which gets the CPO endpoint on assignment.
        // It will call bootstrap() at that time to start the TSOClient
        K2LOG_I(log::tsoclient, "Delaying bootstrap since CPO endpoint is empty");
        return seastar::make_ready_future<>();
    }

    return bootstrap(_cpoEndpoint());
}

seastar::future<> TSOClient::_doGetTSOEndpoints(dto::GetTSOEndpointsRequest &request, std::unique_ptr<TXEndpoint> cpoEP, Duration timeout) {
    return RPC().callRPC<dto::GetTSOEndpointsRequest, dto::GetTSOEndpointsResponse>
            (dto::Verbs::CPO_GET_TSO_ENDPOINTS, request, *cpoEP, timeout)
    .then([this](auto&& response) {
        auto& [status, resp] = response;
        if (!status.is2xxOK() || resp.endpoints.size() == 0) {
            K2LOG_E(log::tsoclient, "Get TSO endpoints failed with status {} and endpoint size {}",
                        status, resp.endpoints.size());
            return seastar::make_exception_future<>(std::runtime_error("Could not bootstrap TSO client"));
        }
        _tsoServerEndpoint = RPC().getTXEndpoint(resp.endpoints[0]);
        return _discoverServiceNodes();
    });
}

seastar::future<> TSOClient::bootstrap(const String& cpoEndpoint) {
    K2LOG_I(log::tsoclient, "start bootstrap with CPO server url: {}", cpoEndpoint);

    _stopped = false;
    _registerMetrics();
    // retry TSO connection if cannot be established
    return seastar::do_with(dto::GetTSOEndpointsRequest{}, ExponentialBackoffStrategy().withRetries(_maxTSORetries()).withStartTimeout(_tsoTimeout()).withRate(2), [cpoEndpoint, this](auto& request, auto& retryStrategy) {
        return retryStrategy.run([&request, cpoEndpoint, this] (size_t retriesLeft, Duration timeout) {
            K2LOG_I(log::tsoclient, "Sending GET_TSO_ENDPOINTS with retriesLeft={}, and timeout={}, with {}", retriesLeft, timeout, cpoEndpoint);
            auto cpoEP = RPC().getTXEndpoint(cpoEndpoint);
            if (!cpoEP) {
                K2LOG_E(log::tsoclient, "CPO endpoint is invalid: {}", cpoEndpoint);
                return seastar::make_exception_future<>(std::runtime_error("Could not bootstrap TSO client"));
            }
            return _doGetTSOEndpoints(request, std::move(cpoEP), timeout);
        })
        .handle_exception([cpoEndpoint, this] (auto exc) {
            K2LOG_W_EXC(log::tsoclient, exc, "Failed to assign TSO for endpoint after retry: {}", cpoEndpoint);
        });
    });
}

seastar::future<> TSOClient::gracefulStop() {
    K2LOG_I(log::tsoclient, "stop");
    if (_stopped) {
        return seastar::make_ready_future<>();
    }

    _stopped = true;
    _initialized=false;

    for (auto& prom : _pendingClientRequests) {
        prom.set_exception(TSOClientShutdownException());
    }
    _pendingClientRequests.clear();

    return std::move(_pendingRequestsWaiter);
}

seastar::future<> TSOClient::_getServiceNodeURLs(Duration timeout){
    return seastar::do_with(GetServiceNodeURLsRequest{}, [this, timeout] (auto& request) {
        return RPC().callRPC<dto::GetServiceNodeURLsRequest, dto::GetServiceNodeURLsResponse>(dto::Verbs::GET_TSO_SERVICE_NODE_URLS, request, *_tsoServerEndpoint, timeout)
        .then([this](auto&& response) {
            auto& [status, r] = response;
            if (!status.is2xxOK()) {
                K2LOG_E(log::tsoclient, "Error during get TSO node URLs, status:{}", status);
                if (status.is5xxRetryable()) {
                    // retryable errors
                    return seastar::make_exception_future<>(std::runtime_error(status.message));
                }
                else {
                    return seastar::make_exception_future<>(StopRetryException{});
                }
            }

            if (r.serviceNodeURLs.empty()) {
                K2LOG_E(log::tsoclient, "Remote end did not provide node URLs");
                return seastar::make_exception_future<>(std::runtime_error("no remote endpoint"));
            }
            else {
                K2LOG_D(log::tsoclient, "received node URLs:{}", r.serviceNodeURLs);
            }

            _curTSOServiceNodes.clear();
            // each node may have multiple endPoint URLs, we only pick the best supported one
            for (auto& singleNodeURLs : r.serviceNodeURLs) {
                _curTSOServiceNodes.push_back(Discovery::selectBestEndpoint(singleNodeURLs));
                K2LOG_D(log::tsoclient, "Selected node endpoint:{}", _curTSOServiceNodes.back()->url);
            }

            K2ASSERT(log::tsoclient, !_curTSOServiceNodes.empty(), "nodes should property configured and not empty!")

            // to reduce run-time computation, we shuffle the _curTSOServiceNodes here
            // to simulate random pick of workers(load balance) in run time by increment a moded index
            std::random_device rd;
            std::mt19937 ranAlg(rd());
            std::shuffle(_curTSOServiceNodes.begin(), _curTSOServiceNodes.end(), ranAlg);

            K2LOG_I(log::tsoclient, "Successfully got remote data endpoint");

            _initialized = true;
            // let all requests know that we're ready
            for (auto& prom: _pendingClientRequests) {
                prom.set_value();
            }
            _pendingClientRequests.clear();
            return seastar::make_ready_future<>();
        }); // rpc call
    }); // do_with request
}

seastar::future<> TSOClient::_discoverServiceNodes() {
    OperationLatencyReporter reporter(_discoveryLatency);  // for reporting metrics

    return seastar::do_with(ExponentialBackoffStrategy().withRetries(_maxTSORetries()).withStartTimeout(_tsoTimeout()).withRate(2), [this](auto& retryStrategy) {
        return retryStrategy.run([this] (size_t retriesLeft, Duration timeout) {
            if (_stopped) {
                K2LOG_I(log::tsoclient, "Stopping retry since we were stopped");
                return seastar::make_exception_future<>(StopRetryException{});
            }
            if (!_tsoServerEndpoint) {
                K2LOG_E(log::tsoclient, "Invalid TSO server endpoint");
                return seastar::make_exception_future(StopRetryException{});
            }
            K2LOG_I(log::tsoclient, "Sending with retriesLeft={}, and timeout={}ms, with {}", retriesLeft, timeout, *_tsoServerEndpoint);
            return _getServiceNodeURLs(timeout);
        }); // strategy run()
    }) // do_with retry strategy
    .finally([reporter=std::move(reporter)] () mutable {
        reporter.report(); // report latency
    });
}

seastar::future<Timestamp> TSOClient::getTimestamp() {
    if (_stopped) {
        K2LOG_I(log::tsoclient, "Stopping issuing timestamp since we were stopped");
        return seastar::make_exception_future<Timestamp>(TSOClientShutdownException());
    }
    OperationLatencyReporter reporter(_latency);  // for reporting metrics

    // TSO client may not yet ready (discover the tso server endpoint), let the request wait in this case.
    if (!_initialized) {
        // if not ready to serve yet
        _pendingClientRequests.emplace_back();
        return _pendingClientRequests.back().get_future()
            .then([this, reporter=std::move(reporter)] () mutable {
                return _getTimestampWithLatency(std::move(reporter));
            });
    }

    return _getTimestampWithLatency(std::move(reporter));
}

seastar::future<Timestamp> TSOClient::_getTimestampWithLatency(OperationLatencyReporter&& reporter) {
    // ExponentialBackoffStrategy doesn't support returning values.
    return seastar::do_with(
        ExponentialBackoffStrategy().withRetries(5).withStartTimeout(10ms).withRate(2),
        GetTimestampRequest{},
        Timestamp(),
        [this] (auto& retryStrategy, auto& request, auto& timestamp) mutable {
        return retryStrategy.run([this, &request, &timestamp] (int retriesLeft, Duration timeout)  mutable {
            if (_stopped) {
                K2LOG_D(log::tsoclient, "Stopping retry since we were stopped");
                return seastar::make_exception_future<>(TSOClientShutdownException());
            }

            K2ASSERT(log::tsoclient, !_curTSOServiceNodes.empty(), "we should have workers");

            if (retriesLeft != 2) {
                // if this is not first try, it means we had error and are retrying, thus change to a new service node.
                _curWorkerIdx++;
            }
            auto& myRemote = _curTSOServiceNodes[_curWorkerIdx % _curTSOServiceNodes.size()];

            K2LOG_D(log::tsoclient, "Requesting timestamp with retriesLeft:{} and timeout:{} to node:{}", retriesLeft, timeout, *myRemote);

            return RPC().callRPC<dto::GetTimestampRequest, dto::GetTimestampResponse>(dto::Verbs::GET_TSO_TIMESTAMP, request, *myRemote, timeout)
            .then([this, &timestamp] (auto&& result) {
                auto& [status, resp] = result;
                if (!status.is2xxOK()) {
                    K2LOG_D(log::tsoclient, "Error during getTimestamp, status:{}", status);
                    return seastar::make_exception_future<>(std::runtime_error(status.message));
                }

                K2LOG_D(log::tsoclient, "got timestamp:{}", resp.timestamp);

                // this is our way of returning a value out of the RetryStrategy
                timestamp = std::move(resp.timestamp);
                return seastar::make_ready_future<>();
            });
        })
        .then_wrapped([&timestamp] (auto&& doneFut) mutable {
            if (doneFut.failed()) {
                K2LOG_W(log::tsoclient, "Failed to get timestamp");
                return seastar::make_exception_future<Timestamp>(doneFut.get_exception());
            }
            // in the happy case, ignore the future from the loop and give out our value future
            doneFut.ignore_ready_future();
            return seastar::make_ready_future<Timestamp>(std::move(timestamp));
        });
    })
    .finally([reporter=std::move(reporter)] () mutable {
        reporter.report();
    });
}

}
