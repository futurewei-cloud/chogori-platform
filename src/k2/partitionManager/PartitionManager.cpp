#include <k2/common/Log.h>
#include "PartitionManager.h"

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
    return seastar::make_ready_future<>();
}

seastar::future<> PartitionManager::start() {
    __local_pmanager = this;
    return seastar::make_ready_future<>();
}

seastar::future<dto::Partition> PartitionManager::assignPartition(const String& cname, dto::Partition partition) {
    (void) cname;
    partition.astate = dto::AssignmentState::Assigned;
    return seastar::make_ready_future<dto::Partition>(std::move(partition));
}

}  // namespace k2
