#pragma once

// third-party
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

#include <k2/transport/TXEndpoint.h>
namespace k2 {
class Discovery {
public :  // application lifespan
    Discovery();
    ~Discovery();

    // This method determines the endpoint for the best match between this node's capabilities and the given URLs.
    static std::unique_ptr<TXEndpoint> selectBestEndpoint(const std::vector<String>& urls);

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();
};  // class Discovery

} // namespace k2
