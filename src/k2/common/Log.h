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
#include <iostream>
#include <seastar/core/reactor.hh>  // for access to reactor
#include <seastar/core/sstring.hh>
#include <sstream>
#include <string>

#include "Chrono.h"
#include "Common.h"
#include "FormattingUtils.h"

// disable unicode formatting for a bit more speed
#undef FMT_UNICODE
#define FMT_UNICODE 0
#include <fmt/compile.h>
#include <fmt/printf.h>

// This file contains some utility macros for logging and tracing,

// performance of stdout with line-flush seems best ~800ns per call.
// For comparison, stderr's performance is ~6000-7000ns
#define K2LOG_STREAM std::cout

#define DO_K2LOG_LEVEL_FMT(level, module, fmt_str, ...)                                                      \
    {                                                                                                        \
        auto id = seastar::engine_is_ready() ? seastar::this_shard_id() : pthread_self();                    \
        fmt::print(K2LOG_STREAM,                                                                             \
                   FMT_STRING("[{}]-{}-({}:{}) [{}] [{}:{} @{}] " fmt_str "\n"),                             \
                   k2::Clock::now(), k2::logging::Logger::procName, module, id,                              \
                   k2::logging::LogLevelNames[to_integral(level)], __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); \
        K2LOG_STREAM << std::flush;                                                                          \
    }

#define K2LOG_LEVEL_FMT(level, logger, fmt_str, ...)                     \
    if (logger.isEnabledFor(level)) {                                    \
        DO_K2LOG_LEVEL_FMT(level, logger.name, fmt_str, ##__VA_ARGS__); \
    }

// allow verbose logging to be compiled-out
#if K2_VERBOSE_LOGGING == 1
#define K2LOG_V(logger, fmt_str, ...) K2LOG_LEVEL_FMT(k2::logging::LogLevel::VERBOSE, logger, fmt_str, ##__VA_ARGS__);
#else
#define K2LOG_V(logger, fmt_str, ...)                                                    \
    if (0) {                                                                             \
        K2LOG_LEVEL_FMT(k2::logging::LogLevel::VERBOSE, logger, fmt_str, ##__VA_ARGS__); \
    }
#endif

#define K2LOG_D(logger, fmt_str, ...) K2LOG_LEVEL_FMT(k2::logging::LogLevel::DEBUG, logger, fmt_str, ##__VA_ARGS__);
#define K2LOG_I(logger, fmt_str, ...) K2LOG_LEVEL_FMT(k2::logging::LogLevel::INFO, logger, fmt_str, ##__VA_ARGS__);
#define K2LOG_W(logger, fmt_str, ...) K2LOG_LEVEL_FMT(k2::logging::LogLevel::WARN, logger, fmt_str, ##__VA_ARGS__);
#define K2LOG_E(logger, fmt_str, ...) K2LOG_LEVEL_FMT(k2::logging::LogLevel::ERROR, logger, fmt_str, ##__VA_ARGS__);
#define K2LOG_F(logger, fmt_str, ...) K2LOG_LEVEL_FMT(k2::logging::LogLevel::FATAL, logger, fmt_str, ##__VA_ARGS__);

#ifndef NDEBUG
// assertion macros which can be compiled-out

#define K2ASSERT(logger, cond, fmt_str, ...)       \
    {                                              \
        if (!(cond)) {                             \
            K2LOG_E(logger, fmt_str, ##__VA_ARGS__); \
            assert((cond));                        \
        }                                          \
    }

#define K2EXPECT(logger, actual, exp) \
    K2ASSERT(logger, (actual) == (exp), "{} == {}", (#actual), (#exp));

#else

#define K2ASSERT(logger, cond, fmt_str, ...)
#define K2EXPECT(logger, actual, exp)
#endif

#define K2LOG_W_EXC(logger, exc, fmt_str, ...)                                     \
    try {                                                                          \
        std::rethrow_exception((exc));                                             \
    } catch (const std::exception& e) {                                            \
        K2LOG_W(logger, fmt_str ": caught exception {}", ##__VA_ARGS__, e.what()); \
    } catch (...) {                                                                \
        K2LOG_W(logger, fmt_str ": caught unknown exception", ##__VA_ARGS__);      \
    }

namespace k2 {
namespace logging {

K2_DEF_ENUM(LogLevel,
    NOTSET,
    VERBOSE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL);

// Users of logging would create an instance of this class at their module level
// Once created, the K2LOG_* macros can be used to perform logging.
// A logger is used to provide
// - a log module name for log identification
// - ability to enable particular log level for particular modules, even in a running system
class Logger {
public:
    // the process name to print in the logs
    static inline String procName;

    // the global (per-thread) log level. This should be initialized at start of process and
    // can be modified while the process is running to affect the current log level
    static inline thread_local LogLevel threadLocalLogLevel = LogLevel::INFO;

    // the per-module (per-thread) log levels. These are the values which have been set either at
    // process start time, or dynamically at runtime. The reason we need a separate map just for the levels is
    // to ensure that if a logger for a module is created lazily, it will discover this level override.
    static inline thread_local std::unordered_map<String, LogLevel> moduleLevels;

    // registry of all active log modules. These are used so that we can notify an active logger at runtime if
    // the log level changes for the particular module
    static inline thread_local std::unordered_map<String, Logger*> moduleLoggers;

    // create logger for a given unique(per-thread) name
    Logger(const char* moduleName) : name(moduleName) {
        assert(moduleLoggers.find(name) == moduleLoggers.end());
        moduleLoggers[name] = this;
        auto it = moduleLevels.find(name);
        if (it != moduleLevels.end()) {
            // there is a per-module level set for this module. Use it instead
            moduleLevel = it->second;
        }

    }
    ~Logger() {
        moduleLoggers.erase(name);
    }
    // see if we should log at the given level
    bool isEnabledFor(LogLevel level) {
        if (moduleLevel > LogLevel::NOTSET) {
            return level >= moduleLevel;
        }
        return level >= threadLocalLogLevel;
    }

    String name; // the name for this logger
    LogLevel moduleLevel = LogLevel::NOTSET; // the module level isn't set by default - use the global level
};

}  // namespace logging
}  // namespace k2
