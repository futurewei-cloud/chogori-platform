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
#define FMT_UNICODE 0

#include <fmt/format.h>
#include <fmt/printf.h>
#include <fmt/compile.h>
#include <nlohmann/json.hpp>
#include "FormattingUtils.h"

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

// timeduration of input timepoint since epoch(machine boot) in nanosec
inline uint64_t nsec_count(const TimePoint& timeP) {
    return nsec(timeP.time_since_epoch()).count();
}
// count of nanosec for steady_clock::now()
inline uint64_t now_nsec_count() {
    return nsec_count(Clock::now());
}
// count of nanosec for system_clock::now()
inline uint64_t sys_now_nsec_count() {
    return nsec(std::chrono::system_clock::now().time_since_epoch()).count();
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
    static inline thread_local TimePoint _now = Clock::now();
};

struct Timestamp_ts {
    uint16_t micros;
    uint16_t millis;
    uint8_t secs;
    uint8_t mins;
    uint8_t hours;
    uint16_t days;
};

inline Timestamp_ts toTimestamp_ts(const TimePoint& tp) {
    auto now = k2::usec(tp.time_since_epoch()).count();
    auto [quotient1, micros] = std::ldiv(now, 1000);
    auto [quotient2, millis] = std::ldiv(quotient1, 1000);
    auto [quotient3, secs] = std::ldiv(quotient2, 60);
    auto [quotient4, mins] = std::div((int)quotient3, 60); // the quotient is small enough to fit in an int now
    auto [days, hours] = std::div(quotient4, 24);
    return {(uint16_t)micros, (uint16_t)millis, (uint8_t)secs, (uint8_t)mins, (uint8_t)hours, (uint16_t)days};
}

inline const char* printTime(const TimePoint& tp) {
    auto ts = toTimestamp_ts(tp);
    static thread_local char buffer[24];
    fmt::format_to_n(buffer, sizeof(buffer), "{}", ts);
    return buffer;
}

} // ns k2

template <>
struct fmt::formatter<k2::Timestamp_ts> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(k2::Timestamp_ts const& ts, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), FMT_COMPILE("{:04}:{:02}:{:02}:{:02}.{:03}.{:03}"), ts.days, ts.hours, ts.mins, ts.secs, ts.millis, ts.micros);
    }
};

template <>
struct fmt::formatter<k2::Duration> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(k2::Duration const& dur, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "{}", k2::TimePoint{} + dur);
    }
};

namespace std {
void inline to_json(nlohmann::json& j, const k2::Duration& obj) {
    j = nlohmann::json{{"duration", k2::usec(obj).count()}};
}

inline void from_json(const nlohmann::json& j, k2::Duration& obj) {
    int64_t result = j.get<int64_t>(); // microseconds
    obj = result * 1us;
}

void inline to_json(nlohmann::json& j, const k2::TimePoint& tp) {
    j = nlohmann::json{{ "timepoint", fmt::format("{}", k2::toTimestamp_ts(tp)) }};
}

inline void from_json(const nlohmann::json& j, k2::TimePoint& tp) {
    // timepoint deserialize expects a json with int64_t microseconds
    int64_t result = j.get<int64_t>(); // microseconds
    tp = k2::TimePoint{} + result * 1us;
}
}

template <>
struct fmt::formatter<k2::TimePoint> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(k2::TimePoint const& tp, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "{}", k2::toTimestamp_ts(tp));
    }
};

namespace std {
inline ostream& operator<<(ostream& os, const k2::TimePoint& o) {
    fmt::print(os, "{}", o);
    return os;
}

inline ostream& operator<<(ostream& os, const k2::Duration& o) {
    fmt::print(os, "{}", o);
    return os;
}

}

namespace k2 {
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
    K2_DEF_FMT(Deadline, _deadline);

private:
    typename ClockT::time_point _deadline;
}; // class Deadline

}
