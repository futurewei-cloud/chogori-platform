#pragma once

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>  // for future stuff

#include <k2/transport/Status.h>
#include <k2/dto/AssignmentManager.h>
#include <k2/dto/Timestamp.h>

namespace k2 {

class AssignmentManager {
public:  // application lifespan
    AssignmentManager();
    ~AssignmentManager();

    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();

    seastar::future<std::tuple<Status, dto::AssignmentCreateResponse>>
    handleAssign(dto::AssignmentCreateRequest&& request);

    seastar::future<std::tuple<Status, dto::AssignmentOffloadResponse>>
    handleOffload(dto::AssignmentOffloadRequest&& request);
};  // class AssignmentManager

} // namespace k2
