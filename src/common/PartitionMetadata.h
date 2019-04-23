#pragma once

#include <map>
#include "Common.h"
#include "CollectionMetadata.h"

namespace k2
{

//
//  Version of partition assignment
//
struct PartitionVersion
{
    uint16_t range;     //  Change with the change of partition range
    uint16_t assign;    //  Change with new partition assignment

    bool operator==(const PartitionVersion& other) const { return range == other.range && assign == other.assign; }
    bool operator!=(const PartitionVersion& other) const { return !(*this == other); }

    K2_PAYLOAD_COPYABLE;
};

//
//  Value of this type uniquely identifies K2 assignment of a partition
//
struct PartitionAssignmentId
{
    PartitionId id;
    PartitionVersion version;

    PartitionAssignmentId() : id(0), version {0, 0} { }
    PartitionAssignmentId(PartitionId id, PartitionVersion version) : id(id), version(version) { }
    PartitionAssignmentId(const PartitionAssignmentId&) = default;
    PartitionAssignmentId& operator=(PartitionAssignmentId& other) = default;

    bool parse(const char* str)
    {
        if(!str || !*str)
            return false;

        static_assert(sizeof(id) == sizeof(uint64_t) && sizeof(version.range) == sizeof(uint16_t) && sizeof(version.assign) == sizeof(uint16_t), "Incorrect PartitionAssignmentId structure");
        return sscanf(str, "%lu.%hu.%hu", &id, &version.range, &version.assign) == 3;
    }

    K2_PAYLOAD_COPYABLE;
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
    PartitionRange& operator=(PartitionRange& other) = default;
    PartitionRange(PartitionRange&& range)  = default;
    PartitionRange& operator=(PartitionRange&& other) = default;

    const String& getLowKey() { return lowKey; }
    const String& getHighKey() { return highKey; }

    K2_PAYLOAD_FIELDS(lowKey, highKey);
};

//
//  Contains partition properties
//
class PartitionMetadata
{
protected:
    PartitionId partitionId;
    PartitionRange range;
    CollectionId collectionId;
public:
    PartitionMetadata() : partitionId(0), collectionId(0) { }

    PartitionMetadata(PartitionId partitionId, PartitionRange&& range, CollectionId collectionId) :
        partitionId(partitionId), range(std::move(range)), collectionId(collectionId) {}

    PartitionMetadata(PartitionMetadata&& other) = default;
    PartitionMetadata& operator=(PartitionMetadata&& other) = default;

    PartitionId getId() const { return partitionId; }
    const PartitionRange& getPartitionRange() const { return range; }
    CollectionId getCollectionId() const { return collectionId; }

    K2_PAYLOAD_FIELDS(partitionId, range, collectionId);
};

}   //  namespace k2
