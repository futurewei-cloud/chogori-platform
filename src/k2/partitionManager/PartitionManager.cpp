#include "PartitionManager.h"
#include <k2/common/Log.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RRDMARPCProtocol.h>
#include <k2/transport/TCPRPCProtocol.h>

namespace k2 {
__thread PartitionManager* __local_pmanager;

PartitionManager::PartitionManager() {
    K2INFO("ctor");
}

PartitionManager::~PartitionManager() {
    K2INFO("dtor");
}

seastar::future<> PartitionManager::stop() {
    K2INFO("stop");
    // signal the partition module that we're stopping
    if (_pmodule) {
        K2INFO("stopping module");
        return _pmodule->stop();
    }
    return seastar::make_ready_future<>();
}

seastar::future<> PartitionManager::start() {
    __local_pmanager = this;
    return seastar::make_ready_future<>();
}

seastar::future<dto::Partition>
PartitionManager::assignPartition(dto::CollectionMetadata meta, dto::Partition partition) {
    if (_pmodule) {
        K2WARN("Partition already assigned");
        partition.astate = dto::AssignmentState::FailedAssignment;
        return seastar::make_ready_future<dto::Partition>(std::move(partition));
    }

    if (meta.storageDriver == dto::StorageDriver::K23SI) {
        partition.astate = dto::AssignmentState::Assigned;

        partition.endpoints.clear();
        auto tcp_ep = k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto);
        if (tcp_ep) {
            partition.endpoints.insert(tcp_ep->getURL());
        }
        auto rdma_ep = k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto);
        if (rdma_ep) {
            partition.endpoints.insert(rdma_ep->getURL());
        }

        _pmodule = std::make_unique<K23SIPartitionModule>(std::move(meta), partition);
        return _pmodule->start().then([partition = std::move(partition)] () mutable {
            if (partition.endpoints.size() > 0) {
                partition.astate = dto::AssignmentState::Assigned;
                K2INFO("Assigned partition for driver k23si");
            }
            else {
                K2ERROR("Server not configured correctly. there were no listening protocols configured");
                partition.astate = dto::AssignmentState::FailedAssignment;
            }
            return seastar::make_ready_future<dto::Partition>(std::move(partition));
        });
    }

    K2WARN("Storage driver not supported: " << meta.storageDriver);
    partition.astate = dto::AssignmentState::FailedAssignment;
    return seastar::make_ready_future<dto::Partition>(std::move(partition));
}

}  // namespace k2
