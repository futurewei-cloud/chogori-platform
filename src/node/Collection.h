#pragma once

#include "common/CollectionMetadata.h"
#include <seastar/core/sharded.hh>

namespace k2
{

//
//  Since collection metadata is shared across all Nodes in a pool we need to make it shareble
//
class CollectionMetadataPtr
{
protected:
    seastar::foreign_ptr<seastar::lw_shared_ptr<CollectionMetadata>> ptr;
public:
    CollectionMetadataPtr() {}

    template <typename... A>
    CollectionMetadataPtr(A&&... a) : ptr(seastar::make_lw_shared(std::forward<A>(a)...)) {}
    const CollectionMetadata& get() { return *ptr.get(); }
};

}   //  namespace k2
