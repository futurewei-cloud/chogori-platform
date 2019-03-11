#pragma once

#include <map>

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
public:
    CollectionMetadata(CollectionId collectionId, Properties properties) : collectionId(collectionId), properties(std::move(properties)) { }

    const CollectionId& getId() { return collectionId; }
    const Properties& getProperties();    
};


}   //  namespace k2
