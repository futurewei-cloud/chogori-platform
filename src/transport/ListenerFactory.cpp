//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <seastar/core/future.hh>

#include "ListenerFactory.h"
#include "Log.h"

namespace k2tx
{
ListenerFactory::ListenerFactory(BuilderFunc_t builder): _builder(builder) {
    K2LOG("");
}

ListenerFactory::~ListenerFactory() {
    K2LOG("");
}

void ListenerFactory::Start() {
    K2LOG("");
    _instance = _builder();
}

seastar::future<> ListenerFactory::stop() {
    K2LOG("");
    if (_instance.get()) {
        return _instance->stop();
    }
    return seastar::make_ready_future<>();
}

} // k2tx
