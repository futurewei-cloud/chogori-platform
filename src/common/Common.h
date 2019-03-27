#pragma once

#include <string>
#include <string_view>
#include <memory>
#include <seastar/core/sstring.hh>

#include "Constants.h"
#include "Status.h"

namespace k2
{

//
//  K2 general string type
//
typedef seastar::sstring String;

//
//  Value of this type uniquely identifies K2 partition
//
typedef uint64_t PartitionId;

//
//  Binary represents owned (not referenced) binary data
//
typedef seastar::temporary_buffer<uint8_t> Binary;

//
//  Slice represents referenced (not owned) binary data
//
typedef seastar::temporary_buffer<uint8_t> Slice;

//
//  Endpoint identifies address of the Node or client. TODO: change to something more appropriate than 'String'.
//
typedef String Endpoint;

//
//  Hold the reference to buffer containing class
//
template<typename T>
class Holder
{
protected:
    Binary data;
public:
    Holder(Binary&& binary)
    {
        assert(data.size() >= sizeof(T));
        data = std::move(binary);
    }

    Holder(Holder&& binary) = default;
    Holder& operator=(Holder&& other) = default;

    T& operator*() { return *(T*)data.get_write(); }
    T* operator->() { return (T*)data.get_write(); }
};

//
//  Check whether some time interval is exceeded
//
class TimeTracker
{
    typedef std::chrono::steady_clock ClockT;
    typedef std::chrono::time_point<ClockT> TimePointT;

    TimePointT startTime;
    std::chrono::nanoseconds timeToTrack;

    static TimePointT now() { return ClockT::now(); }    
public:
    TimeTracker() : startTime(std::chrono::nanoseconds::zero()), timeToTrack(std::chrono::nanoseconds::zero())  {}
    TimeTracker(std::chrono::nanoseconds timeToTrackNS) : startTime(now()), timeToTrack(timeToTrackNS)  {}
    TimeTracker(const TimeTracker& other) = default;

    bool exceeded() { return elapsed() > timeToTrack; };

    std::chrono::nanoseconds remaining()
    {
        TimePointT currentTime = now();
        auto elapsedTime = currentTime - startTime;

        if(elapsedTime > timeToTrack)
            return std::chrono::nanoseconds::zero();

        return timeToTrack - elapsedTime;
    }

    std::chrono::nanoseconds elapsed()
    {
        return now() - startTime;
    }
};

}   //  namespace k2
