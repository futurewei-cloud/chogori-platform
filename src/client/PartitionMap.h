#pragma once

#include <cstdint>
#include "IClient.h"
#include <atomic>

namespace k2
{

//
//  Range that is used in partition map
//  Partition map range always have exclusive high key. Empty high key mean maximum
//  Low key is inclusive unless its empty, which means minumum key.
//
class PartitionMapRange
{
public:
    String lowKey;
    String highKey;

    K2_PAYLOAD_FIELDS(lowKey, highKey);
};

//
//  Describe the partition
//
class PartitionDescription
{
public:
    PartitionMapRange range;
    PartitionAssignmentId id;
    String nodeEndpoint;

    bool operator<(const PartitionDescription& other) noexcept
    {
        return range.lowKey < rhs.range.lowKey;
    }

    K2_PAYLOAD_FIELDS(range, id, nodeEndpoint);
};

//
//  Map of partitions
//
class PartitionMap
{
public:
    std::set<PartitionDescription> map;
    PartitionVersion version;
    std::atomic_long ref_count {0};

    K2_PAYLOAD_FIELDS(map, version);
};

}   //  namespace client
}   //  namespace k2
