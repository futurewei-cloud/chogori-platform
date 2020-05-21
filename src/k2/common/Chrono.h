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

#pragma once
#include <chrono>
#include <iostream>
//
// duration used in a few places to specify timeouts and such
//

using namespace std::chrono_literals;  // so that we can type "1ms"

namespace k2 {

typedef std::chrono::steady_clock Clock;
typedef Clock::duration Duration;
typedef std::chrono::time_point<Clock> TimePoint;

inline std::chrono::nanoseconds nsec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(dur);
}
// timeduration of input timepoint since epoch(machine boot) in nanosec
inline uint64_t nsec_count(const TimePoint& timeP) {
    return nsec(timeP.time_since_epoch()).count();
}
// count of nanosec for steady_clock::now()
inline uint64_t now_nsec_count() {
    return nsec_count(Clock::now());
}
// timeduration of input timepoint since Unix epoch in nanosec
inline uint64_t nsec_count(const std::chrono::time_point<std::chrono::system_clock>& timeP) {
        return nsec(timeP.time_since_epoch()).count();
}
// count of nanosec for system_clock::now()
inline uint64_t sys_now_nsec_count() {
    return nsec_count(std::chrono::system_clock::now());
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


inline const char* printTime(TimePoint tp) {
    // TODO we can use https://en.cppreference.com/w/cpp/chrono/system_clock/to_stream here, but it is a C++20 feature
    static thread_local char buffer[100];
    auto now = k2::usec(tp.time_since_epoch());
    auto microsec = now.count();
    auto millis = microsec/1000;
    microsec -= millis*1000;
    auto secs = millis/1000;
    millis -= secs*1000;
    auto mins = (secs/60);
    secs -= (mins*60);
    auto hours = (mins/60);
    mins -= (hours*60);
    auto days = (hours/24);
    hours -= (days*24);
    std::snprintf(buffer, sizeof(buffer), "%04ld:%02ld:%02ld:%02ld.%03ld.%03ld", days, hours, mins, secs, millis, microsec);
    return buffer;
}
} // ns k2

namespace std {
    inline std::ostream& operator<<(std::ostream& os, const k2::Duration& dur) {
        os << k2::printTime(k2::TimePoint{} + dur);
        return os;
    }

    inline std::ostream& operator<<(std::ostream& os, const k2::TimePoint& tp) {
        os << k2::printTime(tp);
        return os;
    }
} // ns std
