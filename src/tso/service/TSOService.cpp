#include "TSOService.h"
#include "MessageVerbs.h"

#include <common/Log.h>

namespace k2 {

TSOService::TSOService(k2::RPCDispatcher::Dist_t& dispatcher, const bpo::variables_map& config):
    _disp(dispatcher.local()),
    _stopped(false) {
    (void) config; // TODO use the configuration
    K2INFO("ctor");
}

TSOService::~TSOService() {
    K2INFO("dtor");
}

seastar::future<> TSOService::stop() {
    K2INFO("stop");
    if (_stopped) {
        return seastar::make_ready_future<>();
    }
    _stopped = true;
    return seastar::make_ready_future<>();
}

seastar::future<> TSOService::start() {
    _stopped = false;
    K2INFO("Registering message handlers");

    _disp.registerMessageObserver(MsgVerbs::GET,
        [this](k2::Request&& request) mutable {
            (void) request; // TODO do something with the request
        });
    return seastar::make_ready_future<>();
}

} // namespace k2
