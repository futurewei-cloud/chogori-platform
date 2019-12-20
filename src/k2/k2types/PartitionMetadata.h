#pragma once

#include <map>
#include <k2/common/Common.h>
#include "CollectionMetadata.h"

namespace k2
{

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
//  Partition map range always have exclusive high key. Empty high key mean maximum
//
class PartitionRange
{
public: //  TODO: move lowKey and highKey to protected
    String lowKey;
    String highKey;

    PartitionRange() {}
    PartitionRange(String lowKey, String highKey) : lowKey(std::move(lowKey)), highKey(std::move(highKey)) {}
    DEFAULT_COPY_MOVE(PartitionRange);

    const String& getLowKey() const { return lowKey; }
    const String& getHighKey() const { return highKey; }

    K2_PAYLOAD_FIELDS(lowKey, highKey);

    bool operator<(const PartitionRange& other) const noexcept
    {
        return getLowKey() < other.getLowKey();
    }
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

    DEFAULT_COPY_MOVE(PartitionMetadata)

    PartitionId getId() const { return partitionId; }
    const PartitionRange& getPartitionRange() const { return range; }
    CollectionId getCollectionId() const { return collectionId; }

    K2_PAYLOAD_FIELDS(partitionId, range, collectionId);
};



//
//  Describe the partition
//
class PartitionDescription
{
public:
    //  Range that is used in partition map
    //  Partition map range always have exclusive high key. Empty high key mean maximum
    //  Low key is inclusive unless its empty, which means minumum key.
    PartitionRange range;
    PartitionAssignmentId id;
    String nodeEndpoint;

    //DEFAULT_COPY_MOVE_INIT(PartitionDescription)

    K2_PAYLOAD_FIELDS(range, id, nodeEndpoint);

    const String& getLowKey() const { return range.getLowKey(); }
    const String& getHighKey() const { return range.getHighKey(); }

    bool operator<(const PartitionDescription& other) const noexcept
    {
        return range < other.range;
    }
};

//
//  Map of partitions
//
struct PartitionMap
{
    std::set<PartitionDescription> map;
    PartitionVersion version;
    K2_PAYLOAD_FIELDS(map, version);
};

}   //  namespace k2
