#pragma once

// third-party
#include <seastar/core/future.hh>  // for future stuff

namespace k2 {

class PartitionManager {
   public:  // application lifespan
    PartitionManager();
    ~PartitionManager();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();
}; // class PartitionManager

} // namespace k2
