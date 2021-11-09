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
    return steady == o.steady && real == o.real && error == o.error;
}

GPSTimePoint GPSClock::now() {
    _GPSNanos tai;
    {
        // grab the current value via a lock from the volatile global
        std::lock_guard lock{_mutex};
        tai = _lastGPSts;
    }
    const uint64_t elapsed = _steadyNow() - tai.steady;
    const uint64_t drift = getDriftForDuration(elapsed);
    const uint64_t totalError = (uint64_t)tai.error + drift;
    K2LOG_V(log::tsoserver, "gps::now error {}, GPN(real={}, steady={}, error={}), drift={}, elapsed={}", totalError, tai.real, tai.steady, tai.error, drift, elapsed);
    // Do not change the real/steady parts if the timestamp is too old (real hasn't been updated in a while)
    // Instead, we increase the total error incurred by scaling out based on drift.
    // This way it is up to the caller to decide if the error is acceptable or not.
    return GPSTimePoint{.steady = TimePoint{std::chrono::nanoseconds{tai.steady}},
                        .real = TimePoint{std::chrono::nanoseconds{tai.real}},
                        .error = Duration{std::chrono::nanoseconds{std::min(totalError, uint64_t(INF_DRIFT))}}};
}

void GPSClock::poll() {
    // Note: polling is required to ensure that we detect a stale real clock.
    auto nowReal = _gpsNow();
    auto nowSteady = _steadyNow();

    if (nowReal == _lastGPSts.real) {
        // no change in real clock.
        return;
    }

    // we had a change. We should compute the possible error. We do so based on the last values we observed
    //
    // We have a baseline for error (BASE_ERROR_NANOS) which is coming from the gps
    // hardware manufacturer tolerance, and our own calibration
    //
    // In most cases, we expect the change in real time to be ~ change in steady. It is possible however that
    // we were interrupted anywhere above, or there is a weird drift going on with the steady clock.
    // To detect the situation, we compare the change in real with the change in steady
    // (adjusted for our expected drift)
    //
    // The error calculation is based on the delta between the observed change in real and drift-adjusted
    // change in steady. This is called "errorFromExpected" below
    // Possible scenarios:
    // Normal operation:
    //      proportionate delta in both real and steady. errorFromExpected is small
    // We are interrupted before nowReal or between nowReal and nowSteady reads.
    //      This causes a normal delta in real and large delta in steady. errorFromExpected is large
    // The real value stops getting updated
    //      We shortcut this in the check above
    // The real value wasn't updated for a while but now it starts getting updated
    //      the expectedChange adjusted for drift will be large so errorFromExpected is large
    // We get delayed run due to scheduling or interrupt
    //      We'll observe a large delta in real and large delta in steady. errorFromExpected is large
    // The steady clock drift is huge
    //      the delta in steady will be bigger than delta in real so errorFromExpected is large
    auto realChange = nowReal - _lastGPSts.real;
    auto steadyChange = nowSteady - _lastGPSts.steady;
    auto drift = getDriftForDuration(steadyChange);
    if (drift > 1000 && _lastGPSts.steady > 0) {
        K2LOG_W(log::tsoserver, "Drift too large in poll realc={}, stc={}, drift={}", realChange, steadyChange, drift);
    }
    // apply the new TS in memory-coherent way
    uint32_t totalError = INF_DRIFT;
    if (drift != INF_DRIFT) {
        // the drift is uint32 and is less than uint32::max, which means steadyChange is also less than uint32::max
        // adding the two might slightly overflow 32bits so make sure we catch that.
        // subtracting the two is fine since change > drift since
        // drift is guaranteed to be < elapsed time by the helper function
        // promote the change to 64 bit to make sure no overflow can happen with addition
        uint32_t expectedHigh = (uint32_t)std::min((uint64_t)INF_DRIFT, (uint64_t)steadyChange + drift);
        uint32_t expectedLow = (uint32_t)(steadyChange - drift);

        // figure out how far the real change is from the expected change. The larger the changes, the larger
        // the error is going to be.
        uint64_t errorFromExpectedHigh = BASE_ERROR_NANOS + (realChange > expectedHigh ? realChange - expectedHigh : expectedHigh - realChange);
        uint64_t errorFromExpectedLow = BASE_ERROR_NANOS + (realChange > expectedLow ? realChange - expectedLow : expectedLow - realChange);
        auto largerError = std::max(errorFromExpectedHigh, errorFromExpectedLow);
        totalError = (uint32_t)std::min((uint64_t)INF_DRIFT, largerError);
        K2LOG_V(log::tsoserver, "poll stc={}, rc={}, drift={}, nowS={}, nowR={}, err={}, erH={}, erL={}, expH={}, expL={}", steadyChange, realChange, drift, nowSteady, nowReal, totalError, errorFromExpectedHigh, errorFromExpectedLow, expectedHigh, expectedLow);
    }

    std::lock_guard lock{_mutex};
    _lastGPSts = _GPSNanos(nowSteady, nowReal, totalError);
}

void GPSClock::initialize() {
    // in a happy-case situation, we should be able to poll the steady and
    // real values within a base error of each other
    _lastGPSts.error = INF_DRIFT;
    auto start = Clock::now();
    while (_lastGPSts.error > 2 * BASE_ERROR_NANOS) {
        poll();
        auto elapsed = Clock::now() - start;
        if (elapsed > 5s) {
            throw std::runtime_error("Unable to initialize gps clock");
        }
    }
}

uint32_t GPSClock::getDriftForDuration(uint64_t durationNanos) {
    if (durationNanos >= INF_DRIFT || DRIFT >= DRIFT_PERIOD) return INF_DRIFT;  // to prevent overflows
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
