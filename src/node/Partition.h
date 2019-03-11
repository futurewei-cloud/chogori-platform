#pragma once

#include "../common/PartitionMetadata.h"
#include "Collection.h"
#include "persistence/IPersistentLog.h"

namespace k2
{
//
//  K2 Partition
//
class Partition
{
public:
    enum State
    {
        Assigning,
        Offloading,
        Running
    };

protected:
    std::unique_ptr<PartitionMetadata> metadata;
    CollectionMetadataPtr collection;
public:
    Partition(std::unique_ptr<PartitionMetadata> metadata) : metadata(std::move(metadata)) {}

    State state;

    IPersistentLog& getLog(uint32_t logId);
    uint32_t getLogCount();

};  //  class Partition

}   //  namespace k2
