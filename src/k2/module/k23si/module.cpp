#include "module.h"

namespace k2 {

K23SIPartitionModule::K23SIPartitionModule(const String& cname, dto::Partition partition) : _cname(cname), _partition(partition) {
    K2INFO("ctor for cname=" << _cname <<", part=" << _partition);
}

K23SIPartitionModule::~K23SIPartitionModule() {
    K2INFO("dtor for cname=" << _cname <<", part=" << _partition);
}

seastar::future<> K23SIPartitionModule::stop() {
    K2INFO("stop for cname=" << _cname <<", part=" << _partition);
    return seastar::make_ready_future();
}

} // ns k2
