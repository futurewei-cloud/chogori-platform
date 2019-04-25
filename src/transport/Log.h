//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <iostream>
#include <chrono>
#include <ctime>
#include <seastar/core/reactor.hh>

// This file contains some utility macros for logging and tracing, used in the transport
// TODO hook this up into proper logging
#define K2LOG(msg) { \
    char buffer[100]; \
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()); \
    auto microsec = now.count(); \
    auto millis = microsec/1000; \
    microsec -= millis*1000; \
    auto secs = millis/1000; \
    millis -= secs*1000; \
    auto mins = (secs/60); \
    secs -= (mins*60); \
    auto hours = (mins/60); \
    mins -= (hours*60); \
    auto days = (hours/24); \
    hours -= (days*24); \
    std::snprintf(buffer, sizeof(buffer), "%02ld:%02ld:%02ld:%02ld.%03ld.%03ld", days, hours, mins, secs, millis, microsec); \
    std::cerr << "[" << buffer << "] " << "(" << seastar::engine().cpu_id() <<") [" \
    << __FILE__ << ":" <<__LINE__ << " @" << __FUNCTION__ <<"]"  << msg << std::endl; }

#if K2TX_DEBUG == 1
#define K2DEBUG(msg) K2LOG("[DEBUG] " << msg)
#else
#define K2DEBUG(msg)
#endif

// TODO warnings and errors must also emit metrics
#define K2INFO(msg) K2LOG("[INFO] " << msg)
#define K2WARN(msg) K2LOG("[WARN] " << msg)
#define K2ERROR(msg) K2LOG("[ERROR] " << msg)
