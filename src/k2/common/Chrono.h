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
inline std::chrono::microseconds usec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::microseconds>(dur);
}
inline std::chrono::milliseconds msec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(dur);
}
inline std::chrono::seconds sec(const Duration& dur) {
    return std::chrono::duration_cast<std::chrono::seconds>(dur);
}

}

namespace std {
    inline std::ostream& operator<<(std::ostream& os, const k2::Duration& dur) {
        os << dur.count() << "ns";
        return os;
    }
}
