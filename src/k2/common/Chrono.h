#pragma once
#include <chrono>
//
// duration used in a few places to specify timeouts and such
// 

using namespace std::chrono_literals;  // so that we can type "1ms"

namespace k2 {

// TDOD: rename following to SteadyClock, SteadyDuration, SteadyTimePt respectively
typedef std::chrono::steady_clock Clock;
typedef Clock::duration Duration;
typedef std::chrono::time_point<Clock> TimePoint;

inline std::chrono::nanoseconds nsec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(dur);
}
inline std::chrono::microseconds usec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::microseconds>(dur);
}
inline std::chrono::milliseconds msec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(dur);
}
inline std::chrono::seconds sec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::seconds>(dur);
}

typedef std::chrono::system_clock SysClock;
typedef SysClock::duration SysDuration;
typedef SysClock::time_point SysTimePt;

inline constexpr SysTimePt SysTimePt_FromNanoTSE(uint64_t nanoTSE) {
    std::chrono::nanoseconds dur(nanoTSE);
    SysTimePt temp(dur);
    return temp;
}

// removing nano seconds from a system timepoint
inline constexpr uint64_t TSE_Count_MicroSecRounded(const SysTimePt& t) {
    // with -O3 level compiling, following code is same as
    // return (t.time_since_epoch().count() / 1000) * 1000;
    SysTimePt temp(std::chrono::duration_cast<std::chrono::microseconds>(t.time_since_epoch()));
    return temp.time_since_epoch().count();
}

}
