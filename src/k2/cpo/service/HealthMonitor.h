/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>  // for future stuff

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/LogStream.h>
#include <k2/transport/Prometheus.h>
#include <k2/transport/Status.h>
#include <k2/common/Timer.h>

#include "Log.h"

namespace k2::cpo {

// Represents the target of a heartbeat request, which can be anything that responds to a
// Chogori platform RPC request. The data here is informational only and is does not affect the
// heartbeat monitor operation but can be used by other CPO components or for logging purposes
class RPCServer {
public:
    String ID;
    String role;
    String roleMetadata;
    std::vector<String> endpoints;
    K2_DEF_FMT(RPCServer, ID, role, roleMetadata, endpoints);
};

// The data needed for a single target (one-to-one with an RPCServer above) for the heartbeat monitor to
// operate on
class HeartbeatControl {
public:
    TimePoint nextHeartbeat;
    seastar::future<> heartbeatRequest = seastar::make_ready_future<>();
    RPCServer target;
    std::unique_ptr<TXEndpoint> endpoint;
    // Last opaque token received by the target, which is echoed back on the next request.
    // The target uses this to detect if its responses are getting through
    uint64_t lastToken{0};
    uint32_t unackedHeartbeats{0};
    bool heartbeatInFlight{false};
    K2_DEF_FMT(HeartbeatControl, nextHeartbeat, target, lastToken, unackedHeartbeats, heartbeatInFlight);
};

class HealthMonitor {
private:
    ConfigVar<std::vector<String>> _nodepoolEndpoints{"nodepool_endpoints"};
    ConfigVar<std::vector<String>> _TSOEndpoints{"tso_endpoints"};
    ConfigVar<std::vector<String>> _persistEndpoints{"k23si_persistence_endpoints"};
    ConfigVar<Duration> _interval{"heartbeat_interval", 500ms};
    ConfigVar<Duration> _batchWait{"heartbeat_batch_wait", 0s};
    ConfigVar<uint32_t> _batchSize{"heartbeat_batch_size", 100};
    ConfigVar<uint32_t> _deadThreshold{"heartbeat_lost_threshold", 3};
    ConfigVar<uint32_t> _heartbeatMonitorShardId{"heartbeat_monitor_shard_id", 0};

    SingleTimer _nextHeartbeat;
    bool _running{false};

    std::list<seastar::lw_shared_ptr<HeartbeatControl>> _heartbeats;

    // For metrics
    void _registerMetrics();
    sm::metric_groups _metric_groups;
    uint32_t _nodepoolDown{0};
    uint32_t _TSODown{0};
    uint32_t _persistDown{0};
    uint64_t _heartbeatsLost{0};
    uint64_t _downEvents{0};
    k2::ExponentialHistogram _heartbeatLatency;

    const String _nodepoolRole{"Nodepool"};
    const String _tsoRole{"TSO"};
    const String _persistRole{"Persistence"};

    void _addHBControl(RPCServer&& server, TimePoint nextHB);
    void _checkHBs();

    std::vector<String> getNodepoolEndpointsHelper();
    std::vector<String> getTSOEndpointsHelper();
    std::vector<String> getPersistEndpointsHelper();

public:
    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();

    seastar::future<std::vector<String>> getNodepoolEndpoints();
    seastar::future<std::vector<String>> getTSOEndpoints();
    seastar::future<std::vector<String>> getPersistEndpoints();
};  // class HealthMonitor

} // namespace k2
