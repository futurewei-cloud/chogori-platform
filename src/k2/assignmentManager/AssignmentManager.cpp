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

namespace k2 {

AssignmentManager::AssignmentManager() {
    K2LOG_I(log::amgr, "ctor");
}

AssignmentManager::~AssignmentManager() {
    K2LOG_I(log::amgr, "dtor");
}

seastar::future<> AssignmentManager::gracefulStop() {
    K2LOG_I(log::amgr, "stop");
    if (_pmodule) {
        K2LOG_I(log::amgr, "stopping module");
        return _pmodule->gracefulStop();
    }
    return seastar::make_ready_future();
}

seastar::future<> AssignmentManager::start() {
    K2LOG_I(log::amgr, "Registering message handlers");
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
    K2LOG_I(log::amgr, "Received request to create assignment in collection {}, for partition {}", request.collectionMeta.name, request.partition);
    // TODO, consider current load on all cores and potentially re-route the assignment to a different core
    // for now, simply pass it onto local handler

    dto::CollectionMetadata& meta = request.collectionMeta;
    dto::Partition& partition = request.partition;

    if (_pmodule) {
        K2LOG_W(log::amgr, "Partition already assigned");
        partition.astate = dto::AssignmentState::FailedAssignment;
        auto status = Statuses::S403_Forbidden("partition assignment was not allowed");
        dto::AssignmentCreateResponse resp{.assignedPartition = std::move(partition)};
        return RPCResponse(std::move(status), std::move(resp));
    }

    if (meta.storageDriver != dto::StorageDriver::K23SI) {
        K2LOG_W(log::amgr, "Storage driver not supported: {}", meta.storageDriver);
        partition.astate = dto::AssignmentState::FailedAssignment;
        auto status = Statuses::S403_Forbidden("partition assignment was not allowed");
        dto::AssignmentCreateResponse resp{.assignedPartition = std::move(partition)};
        return RPCResponse(std::move(status), std::move(resp));
    }

    partition.astate = dto::AssignmentState::Assigned;

    partition.endpoints.clear();
    auto tcp_ep = k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto);
    if (tcp_ep) {
        partition.endpoints.insert(tcp_ep->url);
    }
    auto rdma_ep = k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto);
    if (rdma_ep) {
        partition.endpoints.insert(rdma_ep->url);
    }

    _pmodule = std::make_unique<K23SIPartitionModule>(std::move(meta), partition);
    return _pmodule->start().then([partition = std::move(partition)] () mutable {
        if (partition.endpoints.size() > 0) {
            partition.astate = dto::AssignmentState::Assigned;
            K2LOG_I(log::amgr, "Assigned partition for driver k23si");
        }
        else {
            K2LOG_E(log::amgr, "Server not configured correctly. there were no listening protocols configured");
            partition.astate = dto::AssignmentState::FailedAssignment;
        }

        auto status = (partition.astate == dto::AssignmentState::Assigned) ?
            Statuses::S201_Created("assignment accepted") :
            Statuses::S403_Forbidden("partition assignment was not allowed");
        dto::AssignmentCreateResponse resp{.assignedPartition = std::move(partition)};
        return RPCResponse(std::move(status), std::move(resp));
    });
}

seastar::future<std::tuple<Status, dto::AssignmentOffloadResponse>>
AssignmentManager::handleOffload(dto::AssignmentOffloadRequest&& request) {
    (void) request;
    // TODO implement - here we should drop our assignment, cleaning up any resources we have
    // allocated for our partition(s). We should be ready to receive new assignments after this.
    K2LOG_I(log::amgr, "Received request to offload assignment for {}", request.collectionName);
    return RPCResponse(Statuses::S501_Not_Implemented("offload has not been implemented"), dto::AssignmentOffloadResponse());
}

}  // namespace k2
