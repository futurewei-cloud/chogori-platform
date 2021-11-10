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
#include <k2/transport/Status.h>
#include <k2/common/Timer.h>

namespace k2 {

class HeartbeatResponder {
private:
    SingleTimer _nextHeartbeatExpire;
    Duration _HBInterval;      // Will be obtained from the heartbeat requests
    uint32_t _HBDeadThreshold; // Will be obtained from the heartbeat requests
    uint32_t _missedHBs{0};
    uint64_t _lastSeq{0};
    bool _running{false};
    bool _up{false};

    // Returned in heartbeat response
    String _ID;
    std::vector<String> _eps;
    String _metadata;

    // For metrics
    void _registerMetrics();
    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _heartbeatInterarrival;
    k2::TimePoint _lastHeartbeatTime;

    seastar::future<std::tuple<Status, dto::HeartbeatResponse>>
    _handleHeartbeat(dto::HeartbeatRequest&& request);

public:
    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();

    // Used by other applets to know if they should soft-down themselves
    bool isUp();
    // Used by other applets to set role-specific metadata,
    // which is passed on to the monitor in the HB response
    void setRoleMetadata(String metadata);
};  // class HealthMonitor

} // namespace k2
