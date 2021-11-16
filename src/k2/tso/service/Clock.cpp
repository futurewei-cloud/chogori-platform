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

#include "Clock.h"
#include "Log.h"

namespace k2::tso {
bool GPSTimePoint::operator==(const GPSTimePoint& o) const {
    return real == o.real && error == o.error;
}

GPSTimePoint GPSClock::now() {
    _GPSNanos tai;
    {
        // grab the current value via a lock from the volatile global
        std::lock_guard lock{_mutex};
        tai = _useTs;
    }
    auto now = _steadyNow();
    return _generateGPSNow(tai, now);
}

// cap a value at INF_ERROR
uint32_t _capVal(uint64_t val) {
    return (uint32_t) std::min(GPSClock::INF_ERROR, val);
}

GPSTimePoint GPSClock::_generateGPSNow(const _GPSNanos& nowGPS, uint64_t nowSteady) {
    const uint64_t elapsed = nowSteady - nowGPS.steady;
    const uint64_t drift = getDriftForDuration(elapsed);
    const uint64_t totalError = _capVal((uint64_t)nowGPS.error + drift);
    K2LOG_V(log::tsoserver, "gps::now error {}, gpn={}, drift={}, elapsed={}", totalError, nowGPS, drift, elapsed);

    return GPSTimePoint{.real = TimePoint{std::chrono::nanoseconds{nowGPS.real + elapsed}},
                        .error = Duration{std::chrono::nanoseconds{totalError}}};
}

void GPSClock::poll() {
    // we need to catch a change in the real clock in as tight as possible window of time
    //  -----steadyLow----OR1----OR2---steadyHigh--->
    // Since the gps clock changed from OR1 to OR2 within the window [steadyLow: steadyHigh], then
    // we can guarantee that the universal time is somewhere in the window OR2 +- error, with
    // error = BASE_ERROR + (steadyHigh-steadyLow) + Drift(steadyHigh-steadyLow)
    auto steadyLow = _steadyNow();
    auto startedAt = steadyLow;
    auto OR1 = _gpsNow();
    auto OR2 = OR1;
    while (1) {
        auto newSteadyLow = _steadyNow();
        OR2 = _gpsNow();
        K2ASSERT(log::tsoserver, OR1 <= OR2, "GPS clock is not monotonic: or1={}, or2={}", OR1, OR2);
        K2ASSERT(log::tsoserver, steadyLow <= newSteadyLow, "Steady clock is not monotonic: sl={}, sh={}", steadyLow, newSteadyLow);
        // liveliness - we must be able to update within this amount of time
        K2ASSERT(log::tsoserver, newSteadyLow - startedAt < LIVELINESS_NANOS, "GPS clock has not updated in {}ms", LIVELINESS_NANOS/1'000'000);

        if (OR1 != OR2) break; // the real clock changed

        // the real clock hasn't changed yet so we can reduce the error window from below
        steadyLow = newSteadyLow;
    }
    // we observed the real value change. Record an upper bound on the error
    auto steadyNow = _steadyNow();

    _updatePolledTimestamp(steadyLow, steadyNow, OR2);
}

// the real clocked changed to the value indicated by realNow, somewhere between steadyLow and steadyNow
void GPSClock::_updatePolledTimestamp(uint64_t steadyLow, uint64_t steadyNow, uint64_t realNow) {
    if (_useTs.real == 0) {
        // update our timestamps if this is the first time we poll
        std::lock_guard lock{_mutex};
        _useTs = _GPSNanos(steadyNow, realNow, INF_ERROR);
        return;
    }

    // see if we should keep using the old TS, or use the new one.

    // the error we'd get if we used the old timestamp
    uint32_t oldErr = _capVal((uint64_t)_useTs.error + getDriftForDuration(steadyNow - _useTs.steady));

    // we observed the real time change somewhere in the steadyChange window, so that's our max error
    uint64_t steadyChange = steadyNow - steadyLow;
    uint32_t drift = getDriftForDuration(steadyChange);
    uint32_t newErr = _capVal(BASE_ERROR_NANOS + steadyChange + drift);

    K2LOG_V(log::tsoserver, "use={}, nowR={}, nowS={}, drift={}, errO={}, errN={}", _GPSNanos(_useTs), realNow, steadyNow, drift, oldErr, newErr);

    // if the new error is too big, just keep using the old timestamp
    if (newErr > oldErr) {
        // warn for liveliness - within some time we should be replacing the old value or something is wrong with the clocks
        if (steadyNow - _useTs.steady > LIVELINESS_NANOS) {
            K2LOG_W(log::tsoserver, "Using last timestamp since it has lower error: use={}, nowR={}, nowS={}, drift={}, errFromOld={}, errFromNew={}", _GPSNanos(_useTs), realNow, steadyNow, drift, oldErr, newErr);
        }
        return;
    }

    // apply the new TS in memory-coherent way
    std::lock_guard lock{_mutex};
    _useTs = _GPSNanos(steadyNow, realNow, newErr);
    return;
}

void GPSClock::initialize(uint64_t maxErrorNanos) {
    // in a happy-case situation, we should be able to poll the steady and
    // real values within a base error of each other
    K2ASSERT(log::tsoserver, INF_ERROR <= std::numeric_limits<uint32_t>::max(), "INF_ERROR must be big enough to fit in a uint32_t value");
    _useTs.error = INF_ERROR;
    auto start = Clock::now();
    while (_useTs.error > maxErrorNanos) {
        poll();
        auto elapsed = Clock::now() - start;
        if ((uint64_t)nsec(elapsed).count() > LIVELINESS_NANOS) {
            throw std::runtime_error("Unable to initialize gps clock");
        }
    }
    K2LOG_I(log::tsoserver, "GPSClock initialized with {}", _GPSNanos(_useTs));
}

uint32_t GPSClock::getDriftForDuration(uint64_t durationNanos) {
    if (durationNanos >= INF_ERROR || DRIFT >= DRIFT_PERIOD) return (uint32_t)INF_ERROR;  // to prevent overflows
    return (DRIFT * durationNanos) / DRIFT_PERIOD;
}

uint64_t GPSClock::_gpsNow() {
    // TODO make this return the current gps value
    // Some assumptions:
    // 1. Timestamp from atomic or GPS source is somehow written to a volatile location in memory
    // 2. We can assume the drift in the GPS source is negligible, and so can assume the error is
    //    a small constant (BASE_ERROR_NANOS)
    // 3. The value gets updated with some cadence (e.g. once every 1-5us or so)

    // simulate the value changing only once per cadence period to avoid modifying the timestamp too often
    uint64_t constexpr updateCadence = BASE_ERROR_NANOS / 2;
    return updateCadence * (_steadyNow() / updateCadence);
}

uint64_t GPSClock::_steadyNow() {
    return nsec(Clock::now()).count();
}

GPSClock::_GPSNanos::_GPSNanos() {
}

GPSClock::_GPSNanos::_GPSNanos(uint64_t steady, uint64_t real, uint32_t error) : steady(steady),
                                                                                 real(real),
                                                                                 error(error) {
}

GPSClock::_GPSNanos::_GPSNanos(const volatile GPSClock::_GPSNanos& o) : steady(o.steady),
                                                                        real(o.real),
                                                                        error(o.error) {
}

GPSClock::_GPSNanos::_GPSNanos(const GPSClock::_GPSNanos& o) : steady(o.steady),
                                                               real(o.real),
                                                               error(o.error) {
}

void GPSClock::_GPSNanos::operator=(const volatile _GPSNanos& o) volatile {
    steady = o.steady;
    real = o.real;
    error = o.error;
}

void GPSClock::_GPSNanos::operator=(const _GPSNanos& o) volatile {
    steady = o.steady;
    real = o.real;
    error = o.error;
}

void GPSClock::_GPSNanos::operator=(_GPSNanos&& o) volatile {
    steady = o.steady;
    real = o.real;
    error = o.error;
}

}
