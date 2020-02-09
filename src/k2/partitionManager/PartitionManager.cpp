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

seastar::future<dto::Partition> PartitionManager::assignPartition(const String& cname, dto::Partition partition) {
    partition.astate = dto::AssignmentState::Assigned;
    partition.endpoints.insert(RPC().getServerEndpoint(TCPRPCProtocol::proto)->getURL());
    if (seastar::engine()._rdma_stack) {
        partition.endpoints.insert(RPC().getServerEndpoint(RRDMARPCProtocol::proto)->getURL());
    }

    _pmodule = std::make_unique<K23SIPartitionModule>(cname, partition);

    return seastar::make_ready_future<dto::Partition>(std::move(partition));
}

}  // namespace k2
