#pragma once

#include <map>
#include "../common/Message.h"
#include <seastar/core/sharded.hh>

namespace k2
{

//
//  Collection metadata. Is shared across all Nodes in a pool.
//  WARNING: if we change it to add polymorphism we need to change CollectionMetadataPtr to use shared_ptr instead of lw_shared_ptr
//
class CollectionMetadata
{
public:
    typedef std::map<String, String> Properties;
protected:
    String collectionName;
    Properties properties;    //  Application defined properties
public:
    CollectionMetadata(String collectionName, Properties properties) : collectionName(std::move(collectionName)), properties(std::move(properties)) { }

    const Properties& getProperties();
    const String& getName() { return collectionName; }
};

//
//  Since collection metadata is shared across all Nodes in a pool we need to make it shareble
//
class CollectionMetadataPtr
{
protected:
    seastar::foreign_ptr<seastar::lw_shared_ptr<CollectionMetadata>> ptr;
public:
    template <typename... A>
    CollectionMetadataPtr(A&&... a) : ptr(seastar::make_lw_shared(std::forward<A>(a)...)) {}
    const CollectionMetadata& get() { return *ptr.get(); }
};

}   //  namespace k2
