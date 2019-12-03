#pragma once

// stl
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <string>

// third-party
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

namespace k2 {
class TSOService {
public: // types
    typedef seastar::distributed<TSOService> Dist_t;
public :  // application lifespan
    TSOService(Dist_t& dist);
    ~TSOService();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();

    seastar::future<> msgReceiver();

private: // members
    Dist_t& _dist;
};  // class TSOService

} // namespace k2
