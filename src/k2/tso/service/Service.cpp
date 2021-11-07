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
    seastar::future<> startFut = seastar::make_ready_future<>();
    if (seastar::this_shard_id() == 0) {
        K2LOG_I(log::tsoserver, "TSOService initializing GPS clock");
        GPSClock::initialize();
        auto now = GPSClock::now();
        K2LOG_I(log::tsoserver, "TSOService clock has been initialized with GPS time: {}", now);
        if (now.error > _errorBound()) {
            K2LOG_E(log::tsoserver, "TSOService cannot start since the GPS error is larger than our error bound {}", _errorBound());
            return seastar::make_exception_future<>(std::runtime_error("gps error is bigger than max error bound"));
        }
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
    // the end count (strictly-increasing) and error for our result
    auto endCount = nsec(_lastGPSTime.real).count() + _currentSequenceOffset;
    auto startDelta = nsec(_lastGPSTime.real).count() - nsec(_lastGPSTime.error).count() + _currentSequenceOffset;

    if (_lastGPSTime.error > _errorBound()) {
        K2LOG_W(log::tsoserver, "large gps error detected: {}", _lastGPSTime);
        return RPCResponse(Statuses::S503_Service_Unavailable("gps error too high at the moment"), dto::GetTimestampResponse{});
    }
    if (_currentSequenceOffset > (uint64_t)nsec(_errorBound()).count()) {
        K2LOG_W(log::tsoserver, "too many ({}) timestamps were generated from last gps timestamp {}", _currentSequenceOffset, _lastGPSTime);
        return RPCResponse(Statuses::S503_Service_Unavailable("too many requests within gps refresh interval"), dto::GetTimestampResponse{});
    }
    return RPCResponse(Statuses::S200_OK("OK"), dto::GetTimestampResponse{.timestamp = dto::Timestamp(endCount, _tsoId, startDelta)});
}

void TSOService::_getGPSNow() {
    auto now = GPSClock::now();
    K2ASSERT(log::tsoserver, now.real >= _lastGPSTime.real, "gps timestamp goes back in time: now={}, last={}", now, _lastGPSTime);
    K2ASSERT(log::tsoserver, now.steady >= _lastGPSTime.steady, "gps timestamp goes back in time: now={}, last={}", now, _lastGPSTime);

    if (now == _lastGPSTime) {
        // GPS clock hasn't changed. Increment a counter and apply it as an offset
        _currentSequenceOffset++;
        K2LOG_W(log::tsoserver, "required count adjustment due to conflicting gps timestamp of {}", _currentSequenceOffset);
    }
    else {
        uint64_t lastRealNanos = nsec(_lastGPSTime.real).count();
        uint64_t nowRealNanos = nsec(now.real).count();
        // GPS clock has changed since last time we issued. Make sure sequencing is maintained
        if (lastRealNanos + _currentSequenceOffset >= nowRealNanos) {
            // we generated too many timestamps from the previous gps time (past the new timestamp)
            // we can reset the sequencing to the overlap
            _currentSequenceOffset = lastRealNanos + _currentSequenceOffset + 1 - nowRealNanos;
        } else {
            // we're past the last timestamp generated. We can safely reset the offset.
            _currentSequenceOffset = 0;
        }
    }

    _lastGPSTime = now;
}

}
