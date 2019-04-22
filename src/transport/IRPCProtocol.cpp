//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "IRPCProtocol.h"
#include "Log.h"

namespace k2tx {

IRPCProtocol::IRPCProtocol(VirtualNetworkStack::Dist_t& vnet, const String& supportedProtocol):
    _vnet(vnet),
    _protocol(supportedProtocol) {
    K2DEBUG("ctor");
    SetMessageObserver(nullptr);
    SetLowTransportMemoryObserver(nullptr);
}

IRPCProtocol::~IRPCProtocol() {
    K2DEBUG("dtor");
}

void IRPCProtocol::SetMessageObserver(MessageObserver_t observer) {
    K2DEBUG("set message observer");
    if (observer == nullptr) {
        K2DEBUG("Setting default message observer");
        _messageObserver = [](Request request) {
            K2WARN("Message: " << request.verb << " from " << request.endpoint.GetURL()
               << " ignored since there is no message observer registered...");
        };
    }
    else {
        _messageObserver = observer;
    }
}

void IRPCProtocol::SetLowTransportMemoryObserver(LowTransportMemoryObserver_t observer) {
    K2DEBUG("set low mem observer");
    if (observer == nullptr) {
        K2DEBUG("Setting default low transport memory observer");
        _lowMemObserver = [](const String& ttype, size_t suggestedBytes) {
            K2WARN("no low-mem observer installed. Transport: "<< ttype << ", requires release of "<< suggestedBytes << "bytes");
        };
    }
    else {
        _lowMemObserver = observer;
    }
}

} // namespace k2tx
