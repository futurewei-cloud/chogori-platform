#pragma once

// stl
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <string>

// third-party
#include <seastar/core/future.hh>        // for future stuff

namespace k2 {
class TSOService {
    public: // public types

    public: // application lifespan
    TSOService();
    ~TSOService();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();
};  // class TSOService

} // namespace k2
