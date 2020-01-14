#pragma once

// third-party
#include <seastar/core/future.hh>  // for future stuff

namespace k2 {

class CollectionMetadataCache {
public:  // application lifespan
    CollectionMetadataCache();
    ~CollectionMetadataCache();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();
};  // class CollectionMetadataCache

} // namespace k2
