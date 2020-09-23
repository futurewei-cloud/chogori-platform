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
#include <pthread.h>
#include <ctime>
#include <string>
#include <iostream>
#include <seastar/core/reactor.hh>  // for access to reactor
#include <seastar/core/sstring.hh>
#include <sstream>
#include "Chrono.h"
namespace k2{
namespace logging {

class LogEntry {
public:
    static seastar::sstring procName;

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
    LogEntry entry;
    auto id = seastar::engine_is_ready() ? seastar::engine().cpu_id() : pthread_self();
    entry.out << "[" << printTime(Clock::now()) << "]-" << LogEntry::procName << "-(" << id <<") ";
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

#define K2WARN_EXC(msg, exc)                                    \
    {                                                           \
        try {                                                   \
            std::rethrow_exception((exc));                      \
        } catch (const std::exception& e) {                     \
            K2WARN(msg << ": caught exception " << e.what());   \
        } catch (...) {                                         \
            K2WARN(msg << ": caught unknown exception");        \
        }                                                       \
    }
#define K2ERROR_EXC(msg, exc)                                    \
    {                                                            \
        try {                                                    \
            std::rethrow_exception((exc));                       \
        } catch (const std::exception& e) {                      \
            K2ERROR(msg << ": caught exception " << e.what());   \
        } catch (...) {                                          \
            K2ERROR(msg << ": caught unknown exception");        \
        }                                                        \
    }

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

#define K2FAIL(msg) { \
    K2ERROR("+++++++++++++++++++++ Expectation failed ++++++++++++++++(" << msg << ")"); \
    throw std::runtime_error("expectation failed"); \
}

#define K2EXPECT(actual, exp) { \
    if (!((actual) == (exp))) { \
        K2ERROR((#actual) << " == " << (#exp)); \
        K2FAIL("actual=" << actual <<", exp="<< exp<< ")"); \
    } \
}


