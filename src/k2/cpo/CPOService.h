#pragma once

// third-party
#include <seastar/core/future.hh>       // for future stuff
#include <seastar/core/distributed.hh>

namespace k2 {
class CPOService {
private:
    typedef std::function<seastar::distributed<CPOService>&()> DistGetter;
    DistGetter _dist;

public:  // application lifespan
    CPOService(DistGetter distGetter);
    ~CPOService();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();
};  // class CPOService

} // namespace k2
