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
    if (meta.storageDriver == dto::StorageDriver::K23SI) {
        _pmodule = std::make_unique<K23SIPartitionModule>(std::move(meta), partition);
        return _pmodule->start().then([partition = std::move(partition)] () mutable {
            auto tcpep = RPC().getServerEndpoint(TCPRPCProtocol::proto);
            if (tcpep) {
                partition.endpoints.insert(tcpep->getURL());
            }
            auto rdmaep = RPC().getServerEndpoint(RRDMARPCProtocol::proto);
            if (rdmaep) {
                partition.endpoints.insert(rdmaep->getURL());
            }
            if (partition.endpoints.size() > 0) {
                partition.astate = dto::AssignmentState::Assigned;
                K2INFO("Assigned partition for driver k23si");
            }
            else {
                K2ERROR("Server not configured correctly. there were no listening protocols configured");
            }
            return seastar::make_ready_future<dto::Partition>(std::move(partition));
        });
    }

    K2WARN("Storage driver not supported: " << (int)meta.storageDriver);
    partition.astate = dto::AssignmentState::FailedAssignment;
    return seastar::make_ready_future<dto::Partition>(std::move(partition));
}

}  // namespace k2
