//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <pthread.h>
#include <ctime>
#include <iostream>
#include <seastar/core/reactor.hh>  // for access to reactor
#include <sstream>
#include "Chrono.h"
namespace k2{
namespace logging {
class LogEntry {
public:
    std::ostringstream out;
    LogEntry()=default;
    LogEntry(LogEntry&&)=default;
    ~LogEntry() {
        // this line should output just the str from the stream since all chained "<<" may cause a thread switch
        // and thus garbled log output
        std::cerr << out.rdbuf()->str();
    }
    template<typename T>
    std::ostringstream& operator<<(const T& val) {
        out << val;
        return out;
    }
private:
    LogEntry(const LogEntry&) = delete;
    LogEntry& operator=(const LogEntry&) = delete;
};

inline LogEntry StartLogStream() {
    // TODO we can use https://en.cppreference.com/w/cpp/chrono/system_clock/to_stream here, but it is a C++20 feature
    static thread_local char buffer[100];
    auto now = k2::usec(Clock::now().time_since_epoch());
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
    LogEntry entry;
    auto id = seastar::engine_is_ready() ? seastar::engine().cpu_id() : pthread_self();
    entry.out << "[" << buffer << "]" << "(" << id <<") ";
    return entry;
}

} // namespace log

} // namepace k2
// This file contains some utility macros for logging and tracing,
// TODO hook this up into proper logging

#define K2LOG(level, msg) { \
    k2::logging::StartLogStream() << "[" << level << "] [" << __FILE__ << ":" << __LINE__ << " @" << __FUNCTION__ << "]" << msg << std::endl; \
    }

#if K2_DEBUG_LOGGING == 1
#define K2DEBUG(msg) K2LOG("DEBUG", msg)
#else
#define K2DEBUG(msg)
#endif

// TODO warnings and errors must also emit metrics
#define K2INFO(msg) K2LOG("INFO", msg)
#define K2WARN(msg) K2LOG("WARN", msg)
#define K2ERROR(msg) K2LOG("ERROR", msg)

#define K2LOG_EXCEPTION(exc) {try { std::rethrow_exception((exc));} catch (const std::exception &e) { K2WARN("Caught exception " << e.what());}}

#ifndef NDEBUG
#define K2ASSERT(cond, msg) \
    {                       \
        if (!(cond)) {      \
            K2ERROR(msg);   \
            assert((cond)); \
        }                   \
    }
#else
#define K2ASSERT(cond, msg)
#endif

#define K2EXPECT(actual, exp) { \
    if (!((actual) == (exp))) { \
        K2ERROR((#actual) << " == " << (#exp)); \
        K2ERROR("+++++++++++++++++++++ Expectation failed ++++++++++++++++( actual=" << actual <<", exp="<< exp<< ")"); \
        throw std::runtime_error("test failed"); \
    } \
}
