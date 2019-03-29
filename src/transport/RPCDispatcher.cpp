//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "RPCDispatcher.h"
#include "Log.h"

namespace k2tx{

RPCDispatcher::RPCDispatcher(ListenerFactory::Dist_t& listeners) : _listeners(listeners) {
    K2LOG("");
}

RPCDispatcher::~RPCDispatcher() { K2LOG(""); }

void RPCDispatcher::Start() { K2LOG(""); }

seastar::future<> RPCDispatcher::stop() {
    K2LOG("");
    return seastar::make_ready_future<>();
}

void RPCDispatcher::RegisterMessageHandler(Verb verb, MessageHandler_t handler) {
    K2LOG("");
    _handlers.emplace(verb, handler);
}

}// namespace k2tx
