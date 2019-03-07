#pragma once

#include <map>
#include "../common/Common.h"
#include "Collection.h"


namespace k2
{
//
//  Contains partition properties
//
class PartitionMetadata
{
protected:
    PartitionId partitionId;
    PartitionRange range;
    CollectionMetadataPtr collection;
};

//
//  K2 Partition
//
class Partition
{
    std::unique_ptr<PartitionMetadata> metadata;
public:
    Partition(std::unique_ptr<PartitionMetadata> metadata) : metadata(std::move(metadata)) {}
};  //  class Partition

}   //  namespace k2
