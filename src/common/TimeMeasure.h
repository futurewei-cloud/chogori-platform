#pragma once

#include <chrono>
#include <common/Log.h>

namespace k2
{

template<typename ClockT = std::chrono::high_resolution_clock>
class Stopwatch
{
public:
    typedef std::chrono::time_point<ClockT> TimePointT;
protected:
    TimePointT startTime;
public:

    static TimePointT now() { return ClockT::now(); }

    Stopwatch() : startTime(now()) { }

    auto elapsed() { return now() - startTime; }

    uint64_t elapsedNS() { return std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed()).count(); }
    uint64_t elapsedUS() { return std::chrono::duration_cast<std::chrono::microseconds>(elapsed()).count(); }
};

class TimeScopeLogger
{
    const char* text;
    Stopwatch<> stopWatch;

    //double get_wall_time()
    //{
    //    struct timeval time;
    //    if (gettimeofday(&time,NULL)){
    //        throw time;
    //    }
    //    return (double)time.tv_sec + (double)time.tv_usec * .000001;
    //}

public:
    TimeScopeLogger(const char* text = "") : text(text), stopWatch() { }

    ~TimeScopeLogger() { K2INFO(text << ": " << stopWatch.elapsedNS() << "ns"); }
};


}   //  namespace k2
