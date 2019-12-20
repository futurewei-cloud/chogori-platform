#pragma once

#include "Chrono.h"
#include "Log.h"

namespace k2
{

class Stopwatch {
protected:
    TimePoint startTime;
public:

    Stopwatch() : startTime(Clock::now()) { }

    auto elapsed() { return Clock::now() - startTime; }

    uint64_t elapsedNS() { return nsec(elapsed()).count(); }
    uint64_t elapsedUS() { return usec(elapsed()).count(); }
};

class TimeScopeLogger
{
    const char* text;
    Stopwatch stopWatch;

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

//
//  Check whether some time interval is exceeded
//
class TimeTracker
{
    Stopwatch stopWatch;
    Duration timeToTrack;

public:
    DEFAULT_COPY_MOVE_INIT(TimeTracker)

    TimeTracker(Duration timeToTrackNS) : timeToTrack(timeToTrackNS)  {}

    bool exceeded() { return elapsed() > timeToTrack; };

    Duration remaining()
    {
        auto elapsedTime = stopWatch.elapsed();
        if(elapsedTime > timeToTrack)
            return Duration::zero();

        return timeToTrack - elapsedTime;
    }

    Duration elapsed() { return stopWatch.elapsed(); }
};

}   //  namespace k2
