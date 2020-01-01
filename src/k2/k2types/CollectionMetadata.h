#pragma once

#include <map>
#include "ModuleId.h"
#include <k2/transport/PayloadSerialization.h>

namespace k2
{

typedef uint64_t CollectionId;
static constexpr CollectionId CollectionsCollectionId = 0;
static constexpr CollectionId PartitionsCollectionId = 1;


//
//  Collection metadata. Is shared across all Nodes in a pool.
//  WARNING: if we change it to add polymorphism we need to change CollectionMetadataPtr to use shared_ptr instead of lw_shared_ptr
//
class CollectionMetadata
{
public:
    typedef std::map<String, String> Properties;
protected:
    CollectionId collectionId;
    Properties properties;    //  Application defined properties
    ModuleId moduleId;
public:
    CollectionMetadata() : collectionId(0), moduleId(ModuleId::None) { }
    CollectionMetadata(CollectionId collectionId, ModuleId moduleId, Properties properties) :
        collectionId(collectionId), properties(std::move(properties)), moduleId(moduleId) { }
    CollectionMetadata(CollectionMetadata&& metadata) = default;
    CollectionMetadata& operator=(CollectionMetadata&& other) = default;


    CollectionId getId() const { return collectionId; }
    const Properties& getProperties() const { return properties; }

    ModuleId getModuleId() const { return moduleId; }

    K2_PAYLOAD_FIELDS(collectionId, moduleId, properties);
};


}   //  namespace k2
