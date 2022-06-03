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

#pragma once
#include <chrono>
#include <climits>
#include <tuple>

// third-party
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

#include <k2/appbase/Appbase.h>
#include <k2/logging/Chrono.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/TSO.h>

namespace k2::tso {
namespace log {
inline thread_local logging::Logger tsoclient("k2::tsoclient");
}

// TSO client lib - providing K2 Timestamp to app
class TSOClient {
public:
    seastar::future<> start();
    seastar::future<> gracefulStop();
    seastar::future<> bootstrap(const String& cpoEndpoint);

    // get a K2 timestamp
    seastar::future<dto::Timestamp> getTimestamp();

private: // metrics
    ExponentialHistogram _latency;
    ExponentialHistogram _discoveryLatency;

    // used to register metrics
    sm::metric_groups _metricGroups;

    // helper
    void _registerMetrics();

private: // methods
    // discover the TSO service nodes (the ones which can provide timestamps) for our current TSO server
    seastar::future<> _discoverServiceNodes();

    // make a remote call to the TSO to discover the worker URLs
    // This method is a helper, used within a RetryStrategy. It can return an exceptional
    // future with StopRetryException to signal that further retries are futile.
    seastar::future<> _getServiceNodeURLs(Duration timeout);

    // Helper used to obtain the timestamp from server and report latency
    seastar::future<dto::Timestamp> _getTimestampWithLatency(OperationLatencyReporter&& reporter);

private: // fields
    ConfigVar<String> _cpoEndpoint{"cpo", ""};

    // to tell if we've been signaled to stop
    bool _stopped{true};

    // to tell if we've initialized the client
    bool _initialized{false};

    // requests that we've received while not initialized - we'll satisfy them after initialization
    std::deque<seastar::promise<>> _pendingClientRequests;
    seastar::future<> _pendingRequestsWaiter = seastar::make_ready_future<>();

    // the TSO server we can contact for timestamps
    std::unique_ptr<TXEndpoint> _tsoServerEndpoint;

    // all URLs of workers of current TSO server
    std::vector<std::unique_ptr<TXEndpoint>> _curTSOServiceNodes;

    // Try to use the same endpoint until there is an error to minimize connection usage
    size_t _curWorkerIdx{0};
};

// operations invalid during server shutdown
class TSOClientShutdownException : public std::exception {};
}
