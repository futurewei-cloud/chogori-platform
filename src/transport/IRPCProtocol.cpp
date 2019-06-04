//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "IRPCProtocol.h"
#include "common/Log.h"

namespace k2 {

IRPCProtocol::IRPCProtocol(VirtualNetworkStack::Dist_t& vnet, const String& supportedProtocol):
    _vnet(vnet),
    _protocol(supportedProtocol) {
    K2DEBUG("ctor");
    setMessageObserver(nullptr);
    setLowTransportMemoryObserver(nullptr);
}

IRPCProtocol::~IRPCProtocol() {
    K2DEBUG("dtor");
}

void IRPCProtocol::setMessageObserver(RequestObserver_t observer) {
    K2DEBUG("set message observer");
    if (observer == nullptr) {
        K2DEBUG("setting default message observer");
        _messageObserver = [](Request&& request) {
            K2WARN("Message: " << request.verb << " from " << request.endpoint.getURL()
               << " ignored since there is no message observer registered...");
        };
    }
    else {
        _messageObserver = std::move(observer);
    }
}

void IRPCProtocol::setLowTransportMemoryObserver(LowTransportMemoryObserver_t observer) {
    K2DEBUG("set low mem observer");
    if (observer == nullptr) {
        K2DEBUG("setting default low transport memory observer");
        _lowMemObserver = [](const String& ttype, size_t suggestedBytes) {
            K2WARN("no low-mem observer installed. Transport: "<< ttype << ", requires release of "<< suggestedBytes << "bytes");
        };
    }
    else {
        _lowMemObserver = std::move(observer);
    }
}

} // namespace k2
