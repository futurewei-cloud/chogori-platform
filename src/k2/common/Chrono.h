#pragma once
#include <chrono>
#include <iostream>
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

// this clock source should be used when you don't care just how precise you are with timing
// and you want to avoid a lot of calls to system's clock.
// It provides monotonically increasing, thread-local sequence of values and refreshes the
// system clock when asked.
struct CachedSteadyClock {
    typedef Duration duration;
    typedef Duration::rep rep;
    typedef Duration::period period;
    typedef TimePoint time_point;
    static const bool is_steady = true;

    static time_point now(bool refresh=false) noexcept;
private:
    static thread_local TimePoint _now;
};

// Utility class to keep track of a deadline. Useful for nested requests
template<typename ClockT=Clock>
class Deadline {
public:
    Deadline(typename ClockT::duration dur) : _deadline(ClockT::now() + dur) {}

    typename ClockT::duration getRemaining() const {
        auto now = ClockT::now();
        if (now >= _deadline) {
            return typename ClockT::duration(0);
        }
        return _deadline - now;
    }

    bool isOver() const {
        return ClockT::now() >= _deadline;
    }

private:
    typename ClockT::time_point _deadline;
}; // class Deadline

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

} // ns k2

namespace std {
    inline std::ostream& operator<<(std::ostream& os, const k2::Duration& dur) {
        os << k2::nsec(dur).count() << "ns";
        return os;
    }
} // ns std
