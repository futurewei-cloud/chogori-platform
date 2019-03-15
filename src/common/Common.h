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
//  Version of partition assignment
//
struct PartitionVersion
{
    uint16_t range;     //  Change with the change of partition range
    uint16_t assign;    //  Change with new partition assignment
};

//
//  Value of this type uniquely identifies K2 assignment of a partition
//
struct PartitionAssignmentId
{
    const PartitionId id;
    const PartitionVersion version;

    PartitionAssignmentId(PartitionId id, PartitionVersion version) : id(id), version(version) { }
};

//
//  Binary represents owned (not referenced) binary data
//
typedef seastar::temporary_buffer<uint8_t> Binary;

//
//  Slice represents referenced (not owned) binary data
//
typedef seastar::temporary_buffer<uint8_t> Slice;

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

//
//  Key space range which defines partition
//
class PartitionRange
{
protected:
    String lowKey;
    String highKey;

public:
    PartitionRange() {}
    PartitionRange(String lowKey, String highKey) : lowKey(std::move(lowKey)), highKey(std::move(highKey)) { }
    PartitionRange(const PartitionRange& range) = default;
    PartitionRange(PartitionRange&& range) : lowKey(std::move(range.lowKey)), highKey(std::move(range.highKey)) { }

    const String& getLowKey() { return lowKey; }
    const String& getHighKey() { return highKey; }
};

}   //  namespace k2
