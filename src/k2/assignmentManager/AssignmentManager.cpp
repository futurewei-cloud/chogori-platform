/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

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

seastar::future<> AssignmentManager::gracefulStop() {
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
    K2INFO("Received request to create assignment in collection " << request.collectionMeta.name
           << ", for partition " << request.partition);
    // TODO, consider current load on all cores and potentially re-route the assignment to a different core
    // for now, simply pass it onto local handler
    return PManager().assignPartition(std::move(request.collectionMeta), std::move(request.partition))
        .then([](auto&& partition) {
            auto status = (partition.astate == dto::AssignmentState::Assigned) ? Statuses::S201_Created("assignment accepted") : Statuses::S403_Forbidden("partition assignment was not allowed");
            dto::AssignmentCreateResponse resp{.assignedPartition = std::move(partition)};
            return RPCResponse(std::move(status), std::move(resp));
        });
}

seastar::future<std::tuple<Status, dto::AssignmentOffloadResponse>>
AssignmentManager::handleOffload(dto::AssignmentOffloadRequest&& request) {
    (void) request;
    // TODO implement - here we should drop our assignment, cleaning up any resources we have
    // allocated for our partition(s). We should be ready to receive new assignments after this.
    K2INFO("Received request to offload assignment for " << request.collectionName);
    return RPCResponse(Statuses::S501_Not_Implemented("offload has not been implemented"), dto::AssignmentOffloadResponse());
}

}  // namespace k2
