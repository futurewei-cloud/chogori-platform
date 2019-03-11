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
public:    
    TimeTracker(std::chrono::nanoseconds timeToTrackInNS) {}   //  TODO: implement
    bool exceeded() { return false; };
    std::chrono::nanoseconds remaining() { return std::chrono::nanoseconds(0); }
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
