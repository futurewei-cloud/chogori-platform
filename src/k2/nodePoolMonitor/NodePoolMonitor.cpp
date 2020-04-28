#include <k2/common/Log.h>
#include "NodePoolMonitor.h"

namespace k2 {

NodePoolMonitor::NodePoolMonitor() {
    K2INFO("ctor");
}

NodePoolMonitor::~NodePoolMonitor() {
    K2INFO("dtor");
}

seastar::future<> NodePoolMonitor::gracefulStop() {
    K2INFO("stop");
    return seastar::make_ready_future<>();
}

seastar::future<> NodePoolMonitor::start() {
    return seastar::make_ready_future<>();
}

}  // namespace k2
