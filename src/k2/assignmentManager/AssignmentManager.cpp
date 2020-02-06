#include "AssignmentManager.h"

#include <k2/common/Log.h>
#include <k2/dto/AssignmentManager.h>   // our DTO
#include <k2/dto/ControlPlaneOracle.h>   // our DTO
#include <k2/dto/MessageVerbs.h>         // our DTO
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/transport/Status.h>         // for RPC
#include <k2/partitionManager/PartitionManager.h> // partition manager

namespace k2 {

AssignmentManager::AssignmentManager() {
    K2INFO("ctor");
}

AssignmentManager::~AssignmentManager() {
    K2INFO("dtor");
}

seastar::future<> AssignmentManager::stop() {
    K2INFO("stop");
    return seastar::make_ready_future();
}

seastar::future<> AssignmentManager::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<dto::AssignmentCreateRequest, dto::AssignmentCreateResponse>(dto::Verbs::K2_ASSIGNMENT_CREATE, [this](dto::AssignmentCreateRequest&& request) {
        return handleAssign(std::move(request));
    });

    RPC().registerRPCObserver<dto::AssignmentOffloadRequest, dto::AssignmentOffloadResponse>(dto::Verbs::K2_ASSIGNMENT_OFFLOAD, [this](dto::AssignmentOffloadRequest&& request) {
        return handleOffload(std::move(request));
    });
    return seastar::make_ready_future<>();
}

seastar::future<std::tuple<Status, dto::AssignmentCreateResponse>>
AssignmentManager::handleAssign(dto::AssignmentCreateRequest&& request) {
    K2INFO("Received request to create assignment in collection " << request.collectionName
           << ", for partition " << request.partition);
    // TODO, consider current load on all cores and potentially re-route the assignment to a different core
    // for now, simply pass it onto local handler
    return PManager().assignPartition(request.collectionName, std::move(request.partition))
        .then([](auto partition) {
            dto::AssignmentCreateResponse resp{.assignedPartition = std::move(partition)};
            return RPCResponse(Status::S201_Created(), std::move(resp));
        });
}

seastar::future<std::tuple<Status, dto::AssignmentOffloadResponse>>
AssignmentManager::handleOffload(dto::AssignmentOffloadRequest&& request) {
    (void) request;
    // TODO implement - here we should drop our assignment, cleaning up any resources we have
    // allocated for our partition(s). We should be ready to receive new assignments after this.
    K2INFO("Received request to offload assignment for " << request.collectionName);
    return RPCResponse(Status::S501_Not_Implemented("offload has not been implemented"), dto::AssignmentOffloadResponse());
}

}  // namespace k2
