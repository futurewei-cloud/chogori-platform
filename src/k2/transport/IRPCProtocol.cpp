/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include "IRPCProtocol.h"
#include <k2/common/Log.h>

namespace k2 {
IAddressProvider::IAddressProvider() {
}

IAddressProvider::~IAddressProvider() {
}

SinglePortAddressProvider::SinglePortAddressProvider(uint16_t port) : port(port) {
}

SocketAddress SinglePortAddressProvider::getAddress(int) const {
    return port;
}

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

const String& IRPCProtocol::supportedProtocol() {
    return _protocol;
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
