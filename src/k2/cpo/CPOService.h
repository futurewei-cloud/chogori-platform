#pragma once

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>  // for future stuff

#include <k2/transport/Status.h>
#include <k2/dto/ControlPlaneOracle.h>

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

    seastar::future<std::tuple<Status, dto::CollectionCreateResponse>>
    handleCreate(dto::CollectionCreateRequest&& request);

    seastar::future<std::tuple<Status, dto::CollectionGetResponse>>
    handleGet(dto::CollectionGetRequest&& request);
};  // class CPOService

} // namespace k2
