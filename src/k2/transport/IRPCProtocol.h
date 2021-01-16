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

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/net/socket_defs.hh>

#include <k2/common/Common.h>
#include "BaseTypes.h"
#include "Request.h"
#include "TXEndpoint.h"
#include "VirtualNetworkStack.h"
#include "Log.h"

namespace k2 {

// This is an interface for an Address Provider. It can be used by protocol builders to bind
// different addresses to different cores.
class IAddressProvider {
public:
    IAddressProvider();
    virtual ~IAddressProvider();

    // this should be implemented by concrete classes. It should return the address for a given coreID
    virtual SocketAddress getAddress(int coreID) const = 0;
};

//
//  Provider which returns address which always belong to single port
//
class SinglePortAddressProvider : public IAddressProvider
{
protected:
    uint16_t port;
public:
    SinglePortAddressProvider(uint16_t port);
    SocketAddress getAddress(int) const override;
};

// This is an interface for RPCProtocols
// RPCProtocols are objects which allow the application to send and receive RPC messages
// in a protocol-specific way.
// NB. RPCProtocols are not meant to be created directly. Instead, you should use a RPCProtocolFactory
// to create protocols in a distributed manner.
class IRPCProtocol {
public: // lifecycle
    // RPCProtocols must be constructed with the distributed<> virtual network stack, and should specify the protocol
    IRPCProtocol(VirtualNetworkStack::Dist_t& vnet, const String& supportedProtocol);

    // destructor
    virtual ~IRPCProtocol();

public: // API
    // Use this method to set the observer for messages from this protocol
    void setMessageObserver(RequestObserver_t observer);

    // This method returns the protocol supported by the implementation
    const String& supportedProtocol();

    // setLowTransportMemoryObserver allows the user to register a observer which will be called when
    // a transport becomes low on memory.
    // The call is triggered every time a transport has to perform allocation of its buffers, and
    // advises the user which transport type requires release of buffers, and what is the required total number
    // of bytes that should be released.
    //
    // The intended use case here is for applications which hold on to Payloads for long periods of time.
    // These applications should register themselves here, and when called should release enough Payloads to satisfy
    // the requiredNumberOfBytes parameter in their callback.
    // Since we're dealing with multiple transports, the callback also indicates which transport protocol
    // required release. The user can then release Payloads whose transport protocol matches.
    void setLowTransportMemoryObserver(LowTransportMemoryObserver_t observer);

    // This method creates an endpoint for a given URL. The endpoint is needed in order to
    // 1. obtain protocol-specific payloads
    // 2. send messages.
    // returns blank pointer if we failed to parse the url or if the protocol is not supported
    virtual std::unique_ptr<TXEndpoint> getTXEndpoint(String url) = 0;

    // Invokes the remote rpc for the given verb with the given payload at the location based on the given endpoint.
    // The message metadata is used to set message features (such as response/request).
    // This is an asyncronous API. No guarantees are made on the delivery of the payload after the call returns.
    virtual void send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata metadata) = 0;

    // Returns the endpoint where this protocol accepts incoming connections.
    virtual seastar::lw_shared_ptr<TXEndpoint> getServerEndpoint() = 0;

public: // distributed<> interface.
    // called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    virtual seastar::future<> stop() = 0;

    // Should be called by user when all distributed objects have been created
    virtual void start() = 0;

protected: // fields
    // our virtual functions
    VirtualNetworkStack::Dist_t& _vnet;

    // the (optional) observer for incoming messages
    RequestObserver_t _messageObserver;

    // the optional observer for low memory events
    LowTransportMemoryObserver_t _lowMemObserver;

    // the protocol supported by this RPCProtocol
    String _protocol;

private: // Not needed
    IRPCProtocol() = delete;
    IRPCProtocol(const IRPCProtocol& o) = delete;
    IRPCProtocol(IRPCProtocol&& o) = delete;
    IRPCProtocol& operator=(const IRPCProtocol& o) = delete;
    IRPCProtocol& operator=(IRPCProtocol&& o) = delete;
};

} // namespace k2
