#pragma once

// third-party
#include <seastar/core/future.hh>  // for future stuff

namespace k2 {

class NodePoolMonitor {
public:  // application lifespan
    NodePoolMonitor();
    ~NodePoolMonitor();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();
};  // class NodePoolMonitor

} // namespace k2
