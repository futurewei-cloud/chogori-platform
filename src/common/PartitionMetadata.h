#pragma once

#include <map>
#include "Common.h"
#include "CollectionMetadata.h"

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
    CollectionId collectionId;
};

}   //  namespace k2
