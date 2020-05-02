#pragma once

// third-party
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

namespace k2 {
class PersistenceService {
public :  // application lifespan
    PersistenceService();
    ~PersistenceService();

    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();
};  // class PersistenceService

} // namespace k2
