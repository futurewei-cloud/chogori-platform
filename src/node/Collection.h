#pragma once

#include "Module.h"
#include "common/CollectionMetadata.h"
#include <seastar/core/sharded.hh>

namespace k2
{
//  Forward declaration
class IModule;
//
//  Store collection related data
//
class Collection
{
protected:
    CollectionMetadata metadata;
    IModule &module;

public:
    Collection(CollectionMetadata &&metadata, IModule &module)
        : metadata(std::move(metadata)), module(module)
    {
    }

    const CollectionMetadata &getMetadata() const { return metadata; }
    IModule &getModule() { return module; }

    CollectionId getId() const { return metadata.getId(); }
};

//
//  Since collection metadata is shared across all Nodes in a pool we need to
//  make it shareble.
//  TODO: keeping ref count so we can remove it from the Pool's collection list
//  when no partition is referencing it.
//
class CollectionPtr
{
protected:
    //  TODO: with SeaStar we need to use foreign_ptr for collection since it
    //  can be allocated in any thread (Node). However for tests if we don't
    //  initialize SeaStar, foreign_ptr will crash the program since reactor is
    //  not initialized. So, lets use unique_ptr for now and then replace it
    //  with foreign_ptr based on macros switch
    // seastar::foreign_ptr<std::unique_ptr<Collection>> ptr;
    std::unique_ptr<Collection> ptr;

public:
    CollectionPtr() {}

    CollectionPtr(CollectionPtr &&other) = default;
    CollectionPtr &operator=(CollectionPtr &&other) = default;

    template <typename... A>
    CollectionPtr(A &&... a)
        : ptr(std::make_unique<Collection>(std::forward<A>(a)...))
    {
    }

    const Collection *get() const { return ptr.get(); }
    Collection *get() { return ptr.get(); }
};

} //  namespace k2
