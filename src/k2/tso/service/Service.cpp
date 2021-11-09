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
#include <k2/appbase/Appbase.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/Timestamp.h>

#include "Exceptions.h"
#include "Service.h"

namespace k2::tso {

TSOService::TSOService() {
    K2LOG_I(log::tsoserver, "ctor");
}

seastar::future<> TSOService::start() {
    K2LOG_I(log::tsoserver, "TSOService start");
    _metricGroups.clear();

    std::vector<sm::label_instance> labels;

    _metricGroups.add_group("TSO", {
        sm::make_histogram("timetamp_error", [this]{ return _timestampErrors.getHistogram();},
                sm::description("Errors in returned timestamp in nanoseconds"), labels)
    });

    seastar::future<> startFut = seastar::make_ready_future<>();
    if (seastar::this_shard_id() == 0) {
        K2LOG_I(log::tsoserver, "TSOService initializing GPS clock");
        GPSClock::initialize();
        auto now = GPSClock::now();
        K2LOG_I(log::tsoserver, "TSOService clock has been initialized with GPS steady={}, real={}, error={}",
                     nsec(now.steady).count(), nsec(now.real).count(), nsec(now.error).count());
        if (now.error > _errorBound()) {
            K2LOG_E(log::tsoserver, "TSOService cannot start since the GPS error is larger than our error bound {}", _errorBound());
            return seastar::make_exception_future<>(std::runtime_error("gps error is bigger than max error bound"));
        }
        // start the poller thread
        K2LOG_I(log::tsoserver, "Starting GPS clock poller on CPU: {}", _clockPollerCPU());
        _keepRunningPoller.test_and_set(); // set the running flag to true
        _clockPoller = std::thread([this, pinCPU=_clockPollerCPU()] {
            // pin the calling thread to the given CPU
            if (pinCPU >= 0) {
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(pinCPU, &cpuset);
                if (0 != pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset)) {
                    throw std::runtime_error("Unable to set affinity");
                }
            }
            while (_keepRunningPoller.test_and_set()) {
                GPSClock::poll();
            }
        });
        startFut = startFut.then([] {
            // notify all workers that the clock is initialized and we can start
            return k2::AppBase().getDist<TSOService>().invoke_on_all([] (auto& svc) {
                svc._clockInitialized.set_value();
            });
        });
    }

    return startFut
        .then([this] {
            // wait for clock to be initialized
            return _clockInitialized.get_future();
        })
        .then([this] {
            return _collectWorkerURLs();
        })
        .then([this] {
            // TODO also calibrate local error bound
            return _cpoRegister();
        })
        .then([this] {
            // register RPC APIs
            RPC().registerRPCObserver<dto::GetServiceNodeURLsRequest, dto::GetServiceNodeURLsResponse>
            (dto::Verbs::GET_TSO_SERVICE_NODE_URLS, [this](dto::GetServiceNodeURLsRequest&& request) {
                return _handleGetServiceNodeURLs(std::move(request));
            });

            RPC().registerRPCObserver<dto::GetTimestampRequest, dto::GetTimestampResponse>(dto::Verbs::GET_TSO_TIMESTAMP, [this](dto::GetTimestampRequest&& request) {
                return _handleGetTimestamp(std::move(request));
            });

            K2LOG_I(log::tsoserver, "TSOService started");
            return seastar::make_ready_future<>();
        });
}

seastar::future<> TSOService::gracefulStop() {
    K2LOG_I(log::tsoserver, "TSOService stop");
    if (_clockPoller.joinable()) {
        K2LOG_I(log::tsoserver, "Stopping clock poller");
        _keepRunningPoller.clear(); // set the flag to false to signal the thread to exit
        _clockPoller.join();
        K2LOG_I(log::tsoserver, "Clock poller stopped");
    }

    K2LOG_I(log::tsoserver, "done cleaning up");

    return seastar::make_ready_future<>();
}

seastar::future<> TSOService::_collectWorkerURLs() {
    K2LOG_I(log::tsoserver, "collecting all worker URLs");
    std::vector<String> myurls;
    for (auto& ep : RPC().getServerEndpoints()) {
        myurls.push_back(ep->url);
    };

    return AppBase().getDist<TSOService>().invoke_on_all([myurls] (auto& svc) {
        svc._workersURLs.push_back(myurls);
    });
}

seastar::future<> TSOService::_cpoRegister() {
    // TODO implement CPO-based registration and ID assignment
    _tsoId = seastar::this_shard_id() + 100000;

    K2LOG_I(log::tsoserver, "Registration with CPO successful with id {}", _tsoId);
    return seastar::make_ready_future<>();
}

seastar::future<std::tuple<Status, dto::GetServiceNodeURLsResponse>>
TSOService::_handleGetServiceNodeURLs(dto::GetServiceNodeURLsRequest&&) {
    K2LOG_D(log::tsoserver, "handleGetServiceNodeURLs");
    if (_tsoId == 0) {
        return RPCResponse(Statuses::S410_Gone("this server is not authorized to generate timestamps"), dto::GetServiceNodeURLsResponse{});
    }
    dto::GetServiceNodeURLsResponse response{.serviceNodeURLs = _workersURLs};
    K2LOG_D(log::tsoserver, "returned TSO service nodes endpoints are: {}", response);
    return RPCResponse(Statuses::S200_OK("OK"), std::move(response));
}

seastar::future<std::tuple<Status, dto::GetTimestampResponse>>
TSOService::_handleGetTimestamp(dto::GetTimestampRequest&& request) {
    (void)request; // nothing is passed in
    if (_tsoId == 0) {
        return RPCResponse(Statuses::S410_Gone("this server is not authorized to generate timestamps"), dto::GetTimestampResponse{});
    }
    _getGPSNow();
    _timestampErrors.add(nsec(_lastGPSTime.error).count());

    if (_lastGPSTime.error > _errorBound()/2) {
        // no need to bother doing anything else - the error in gps is too high
        K2LOG_W(log::tsoserver, "large gps error detected: {}", _lastGPSTime);
        return RPCResponse(Statuses::S503_Service_Unavailable("gps error too high at the moment"), dto::GetTimestampResponse{});
    }
    // We now have to map a GPS timepoint (real +-error) to a K2 Timestamp([endCount-delta: endCount]).
    // Although gps.real is a monotonically-increasing value, we cannot simply assign it as endCount since
    // the true time may be bigger than gps.real (e.g. gps.real + error).
    // Instead, we produce a timestamp which is guaranteed to contain gps.real +- gps.error.
    // GPS:           |          |           |
    //                 <error> <real> <error>
    // TS:      |                                       |
    //              <delta>                     <endCount>
    // The requirement for the timestamp then become:
    // 0. endCount > lastEndCount           // strictly-increasing
    // 1. real + error <= endCount          // contain upperbound of gps time
    // 2. real - error >= endCount - delta  // contain lowerbound of gps time
    // 3. delta <= errorBound               // the error in the timestamp is no bigger than our advertised error bound
    //
    // To achieve this, we use the fact that we're allowed to produce all of our timestamps with constant max error
    uint64_t delta = nsec(_errorBound()).count(); // condition #3: error is no-greater than error bound
    // and then we use the monotonic gps.real value, shifting it by a constant so that it remains monotonic
    uint64_t endCount = nsec(_lastGPSTime.real).count() + delta/2;
    // Thus Timestamp([endCount - error, error]) is guaranteed to include the entirety of the gps time

    // condition #0: now we have to make sure that endCount is strictly-increasing
    // FYI, probably the GPS timestamp did not update across consecutive calls to TSO.
    if (endCount <= _lastGeneratedEndCount) {
        // increment the last returned value and use it as the end count
        ++_lastGeneratedEndCount;
        endCount = _lastGeneratedEndCount;
    }

    // ensure the result is a valid TSO timestamp within error bound
    if ((uint64_t)nsec(_lastGPSTime.real - _lastGPSTime.error).count() < endCount - delta ||
        (uint64_t)nsec(_lastGPSTime.real + _lastGPSTime.error).count() > endCount) {
        // condition #1 and #2: Timestamp is guaranteed to contain the GPSTime including any error there
        K2LOG_W(log::tsoserver, "large gps error detected: {}", _lastGPSTime);
        return RPCResponse(Statuses::S503_Service_Unavailable("gps error too high at the moment"), dto::GetTimestampResponse{});
    }

    // we're done generating a new timestamp. Remember the end count for next time
    _lastGeneratedEndCount = endCount;
    return RPCResponse(Statuses::S200_OK("OK"), dto::GetTimestampResponse{.timestamp = dto::Timestamp(endCount, _tsoId, delta)});
}

void TSOService::_getGPSNow() {
    auto now = GPSClock::now();

    K2ASSERT(log::tsoserver, now.real >= _lastGPSTime.real, "gps timestamp goes back in time: now={}, last={}", now, _lastGPSTime);
    K2ASSERT(log::tsoserver, now.steady >= _lastGPSTime.steady, "gps timestamp goes back in time: now={}, last={}", now, _lastGPSTime);

    _lastGPSTime = now;
}

}
