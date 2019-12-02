#include "TSOService.h"
#include "MessageVerbs.h"

#include <common/Log.h>
#include <transport/RPCDispatcher.h> // for RPC

namespace k2 {

TSOService::TSOService() {
    K2INFO("ctor");
}

TSOService::~TSOService() {
    K2INFO("dtor");
}

seastar::future<> TSOService::stop() {
    K2INFO("stop");
    return seastar::make_ready_future<>();
}

seastar::future<> TSOService::start() {
    K2INFO("Registering message handlers");

    RPC.local().registerMessageObserver(MsgVerbs::GET,
        [this](k2::Request&& request) mutable {
            (void) request; // TODO do something with the request
        });
    return seastar::make_ready_future<>();
}

} // namespace k2
