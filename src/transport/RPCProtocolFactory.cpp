//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <seastar/core/future.hh>

#include "RPCProtocolFactory.h"
#include "common/Log.h"

namespace k2 {
RPCProtocolFactory::RPCProtocolFactory(BuilderFunc_t builder): _builder(builder) {
    K2DEBUG("ctor");
}

RPCProtocolFactory::~RPCProtocolFactory() {
    K2DEBUG("dtor");
}

void RPCProtocolFactory::Start() {
    K2DEBUG("start");
    // Create the protocol instance
    _instance = _builder();
    if (_instance) {
        _instance->Start();
    }
}

seastar::future<> RPCProtocolFactory::stop() {
    K2DEBUG("stop");
    if (_instance) {
        // pass-on the signal to stop
        auto result = _instance->stop();
        _instance = nullptr;
        return result;
    }
    return seastar::make_ready_future<>();
}

} // k2
