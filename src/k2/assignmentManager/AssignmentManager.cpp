#include "AssignmentManager.h"

#include <k2/common/Log.h>
#include <k2/dto/AssignmentManager.h>   // our DTO
#include <k2/dto/MessageVerbs.h>         // our DTO
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/transport/Status.h>         // for RPC

namespace k2 {

AssignmentManager::AssignmentManager() {
    K2INFO("ctor");
}

AssignmentManager::~AssignmentManager() {
    K2INFO("dtor");
}

seastar::future<> AssignmentManager::stop() {
    K2INFO("stop");
    return seastar::make_ready_future<>();
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
    (void) request;
    K2INFO("Received request to assign partition");
    return RPCResponse(Status::S501_Not_Implemented("assignment create has not been implemented"), dto::AssignmentCreateResponse());
}

seastar::future<std::tuple<Status, dto::AssignmentOffloadResponse>>
AssignmentManager::handleOffload(dto::AssignmentOffloadRequest&& request) {
    (void) request;
    K2INFO("Received request to offload partition");
    return RPCResponse(Status::S501_Not_Implemented("offload has not been implemented"), dto::AssignmentOffloadResponse());
}

}  // namespace k2
