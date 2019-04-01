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

    K2_PAYLOAD_COPYABLE

    bool operator==(const PartitionVersion& other) const { return range == other.range && assign == other.assign; }
    bool operator!=(const PartitionVersion& other) const { return !(*this == other); }
};

//
//  Value of this type uniquely identifies K2 assignment of a partition
//
struct PartitionAssignmentId
{
    PartitionId id;
    PartitionVersion version;

    K2_PAYLOAD_COPYABLE

    PartitionAssignmentId(PartitionId id, PartitionVersion version) : id(id), version(version) { }
    PartitionAssignmentId(const PartitionAssignmentId&) = default;
    PartitionAssignmentId& operator=(PartitionAssignmentId& other) = default;
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

    K2_PAYLOAD_FIELDS(partitionId, range, collectionId);

    PartitionId getId() const { return partitionId; }
    const PartitionRange& getPartitionRange() const { return range; }
    CollectionId getCollectionId() const { return collectionId; }
};

}   //  namespace k2
