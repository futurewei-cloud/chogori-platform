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
#include <k2/dto/TSO.h>

#include "Clock.h"
#include "Log.h"
#include "k2/dto/ControlPlaneOracle.h"
#include <thread>
#include <k2/appbase/AppEssentials.h>

namespace k2::tso {

// This is the TSO service. It is meant to be started in a seastar::distributed<> container,
// where each core can
// 1. return the URLS for all cores as potential timestamp generators
// 2. produce a K2 Timestamp
class TSOService {
public:
    TSOService();

    // start the service
    seastar::future<> start();

    // stop the service
    seastar::future<> gracefulStop();

private: // API
    seastar::future<std::tuple<k2::Status, dto::GetServiceNodeURLsResponse>>
    _handleGetServiceNodeURLs(dto::GetServiceNodeURLsRequest&& request);

    seastar::future<std::tuple<k2::Status, dto::GetTimestampResponse>>
    _handleGetTimestamp(dto::GetTimestampRequest&& request);

private: // methods
    // obtain the worker URLs by contacting each worker core
    seastar::future<> _collectWorkerURLs();

    // obtain the TSOID and the error bound value from the CPO
    seastar::future<std::tuple<Status, dto::AssignTSOResponse>> 
    _handleAssignment(dto::AssignTSORequest&& request);

    // handler for tsoID and error bound assignment, does the local time check
    seastar::future<> _assign(uint64_t tsoID, k2::Duration errBound);

private: // members
    // we use this to signal from core 0 to all workers that the GPS clock has been initialized
    seastar::promise<> _clockInitialized;

    // the last endCount we generated from this service
    uint64_t _lastGeneratedEndCount{0};

    // the URLs for all workers at this service. Each worker can have multiple URLs (e.g. TCP and RDMA)
    std::vector<std::vector<k2::String>> _workersURLs;

    // the TSO id for this service worker. Each worker gets its own globally unique ID, which
    // is assigned by the CPO at registration time.
    // 0 is a special value which indicates that this TSO worker is currently not registered with the CPO
    // which makes it an instance not authorized to produce timestamps in the cluster.
    uint64_t _tsoId{0};

    // set by the CPO
    Duration _CPOErrorBound{0};

    // to poll the gps clock
    std::thread _clockPoller;
    // flag to signal the poller to stop
    std::atomic_flag _keepRunningPoller = ATOMIC_FLAG_INIT;

    // metrics
    sm::metric_groups _metricGroups;
    k2::ExponentialHistogram _timestampErrors;

private: // config
    // The error bound for uncertainty for timestamps
    // The minimum transaction latency (see K23SI design doc) is derived from this number; essentially
    // MTL = max( {TSO.uncertaintyWindow | for all TSOs} )
    // Returned timestamps are guaranteed to contain the true time in the window [endCount - error_bound, endCount]
    ConfigDuration _errorBound{"tso.error_bound", 20us};    // TODO: cleanup?

    // pick CPU on which to pin clock poller (default(-1) is free-floating)
    ConfigVar<int16_t> _clockPollerCPU{"tso.clock_poller_cpu", -1};
};

}
