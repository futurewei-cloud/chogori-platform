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
#include <k2/transport/RPCDispatcher.h>  // for RPC

#include "Clock.h"
#include "Log.h"

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

    // Register with the CPO as an authorized TSO server
    seastar::future<> _cpoRegister();

    // gets the latest GPS clock timestamp and sets up the
    // offset needed to produce the next timestamp in sequence
    void _getGPSNow();

private: // members
    // we use this to signal from core 0 to all workers that the GPS clock has been initialized
    seastar::promise<> _clockInitialized;

    // keep track of the last GPS timestamp we saw so that we can guarantee strictly-increasing sequence
    GPSTimePoint _lastGPSTime;

    // keep track of the current offset we need to apply to a GPS timestamp if it hasn't changed
    uint64_t _currentSequenceOffset{0};

    // the URLs for all workers at this service. Each worker can have multiple URLs (e.g. TCP and RDMA)
    std::vector<std::vector<k2::String>> _workersURLs;

    // the TSO id for this service worker. Each worker gets its own globally unique ID, which
    // is assigned by the CPO at registration time.
    // 0 is a special value which indicates that this TSO worker is currently not registered with the CPO
    // which makes it an instance not authorized to produce timestamps in the cluster.
    uint64_t _tsoId{0};

private: // config
    // The error bound for uncertainty for timestamps
    // The minimum transaction latency (see K23SI design doc) is derived from this number; essentially
    // MTL = max( {TSO.uncertaintyWindow | for all TSOs} )
    ConfigDuration _errorBound{"tso.error_bound", 10us};
};

}
