//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <seastar/core/future.hh>

#include "ListenerFactory.h"
#include "Log.h"

namespace k2tx
{
ListenerFactory::ListenerFactory(BuilderFunc_t builder): _builder(builder) {
    K2DEBUG("");
}

ListenerFactory::~ListenerFactory() {
    K2DEBUG("");
}

void ListenerFactory::Start() {
    K2DEBUG("");
    _instance = _builder();
}

seastar::future<> ListenerFactory::stop() {
    K2DEBUG("");
    if (_instance.get()) {
        return _instance->stop();
    }
    return seastar::make_ready_future<>();
}

} // k2tx
