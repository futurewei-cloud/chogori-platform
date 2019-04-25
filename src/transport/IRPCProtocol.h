//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <seastar/core/distributed.hh>

#include "VirtualNetworkStack.h"
#include "Endpoint.h"
#include "Request.h"
#include "BaseTypes.h"

namespace k2tx {

// This is an interface for RPCProtocols
// RPCProtocols are objects which allow the application to send and receive RPC messages
// in a protocol-specific way.
// NB. RPCProtocols are not meant to be created directly. Instead, you should use a RPCProtocolFactory
// to create protocols in a distributed manner.
class IRPCProtocol {
public: // types
    // A message observer is a function which receives a request
    typedef std::function<void(Request)> MessageObserver_t;

    // The type of observer for low memory events. The caller is provided with the transport protocol that requires
    // release, and the required number of bytes to release
    typedef std::function<void(const String&, size_t)> LowTransportMemoryObserver_t;

public: // lifecycle
    // RPCProtocols must be constructed with the distributed<> virtual network stack, and should specify the protocol
    IRPCProtocol(VirtualNetworkStack::Dist_t& vnet, const String& supportedProtocol);

    // destructor
    virtual ~IRPCProtocol();

public: // API
    // Use this method to set the observer for messages from this protocol
    void SetMessageObserver(MessageObserver_t observer);

    // This method returns the protocol supported by the implementation
    const String& SupportedProtocol() { return _protocol;}

    // SetLowTransportMemoryObserver allows the user to register a observer which will be called when
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
    void SetLowTransportMemoryObserver(LowTransportMemoryObserver_t observer);

    // This method creates an endpoint for a given URL. The endpoint is needed in order to
    // 1. obtain protocol-specific payloads
    // 2. send messages.
    // returns blank pointer if we failed to parse the url or if the protocol is not supported
    virtual std::unique_ptr<Endpoint> GetEndpoint(String url) = 0;

    // Invokes the remote rpc for the given verb with the given payload at the location based on the given endpoint.
    // The message metadata is used to set message features (such as response/request).
    // This is an asyncronous API. No guarantees are made on the delivery of the payload after the call returns.
    virtual void Send(Verb verb, std::unique_ptr<Payload> payload, Endpoint& endpoint, MessageMetadata metadata) = 0;

public: // distributed<> interface.
    // called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    virtual seastar::future<> stop() = 0;

    // Should be called by user when all distributed objects have been created
    virtual void Start() = 0;

protected: // fields
    // our virtual functions
    VirtualNetworkStack::Dist_t& _vnet;

    // the (optional) observer for incoming messages
    MessageObserver_t _messageObserver;

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

} // namespace k2tx