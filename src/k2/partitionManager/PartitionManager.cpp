#include <k2/common/Log.h>
#include "PartitionManager.h"

namespace k2 {

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
    return seastar::make_ready_future<>();
}

}  // namespace k2
