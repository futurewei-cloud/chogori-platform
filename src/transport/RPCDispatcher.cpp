//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "RPCDispatcher.h"
#include "Log.h"

namespace k2tx{

RPCDispatcher::RPCDispatcher(ListenerFactory::Dist_t& listeners) : _listeners(listeners) {
    K2DEBUG("");
}

RPCDispatcher::~RPCDispatcher() { K2DEBUG(""); }

void RPCDispatcher::Start() { K2DEBUG(""); }

seastar::future<> RPCDispatcher::stop() {
    K2DEBUG("");
    return seastar::make_ready_future<>();
}

void RPCDispatcher::RegisterMessageHandler(Verb verb, MessageHandler_t handler) {
    K2DEBUG("");
    _handlers.emplace(verb, handler);
}

}// namespace k2tx
