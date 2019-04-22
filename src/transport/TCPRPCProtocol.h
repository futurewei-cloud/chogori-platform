//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
// third-party
#include <seastar/core/distributed.hh> // distributed stuff
#include <seastar/core/shared_ptr.hh> // pointers (shared/lw)
#include <seastar/core/weak_ptr.hh> // pointers (weak)
#include <seastar/net/socket_defs.hh> // for socket_address

// k2tx
#include "IRPCProtocol.h"
#include "VirtualNetworkStack.h"
#include "RPCProtocolFactory.h"
#include "TCPRPCChannel.h"
#include "RPCHeader.h"

namespace k2tx {

// TCPRPCProtocol is a protocol which use the currently configured TCP stack, with responsibility to:
// - listen for incoming TCP connections
// - create outgoing TCP connections when asked to send messages
// - receive incoming messages and pass them on to the message observer for the protocol
// NB, the class is meant to be used as a distributed<> container
class TCPRPCProtocol: public IRPCProtocol, public seastar::weakly_referencable<TCPRPCProtocol> {
public: // types
    // TODO allow listening on multiple ports with this proto. For now, we allow just one
    static RPCProtocolFactory::BuilderFunc_t Builder(VirtualNetworkStack::Dist_t& vnet, uint16_t port);

    // The official protocol name supported for communications over TCPRPC channels
    static const String proto;

public: // lifecycle
    // Construct the protocol with a vnet which supports TCP
    TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet, uint16_t port);

    // Destructor
    virtual ~TCPRPCProtocol();

public: // API

    // This method creates an endpoint for a given URL. The endpoint is needed in order to
    // 1. obtain protocol-specific payloads
    // 2. send messages.
    // returns blank pointer if we failed to parse the url or if the protocol is not supported
    std::unique_ptr<Endpoint> GetEndpoint(String url) override;

    // Invokes the remote rpc for the given verb with the given payload. This is an asyncronous API. No guarantees
    // are made on the delivery of the payload after the call returns.
    // This is a lower-level API which is useful for sending messages that do not expect replies.
    // The RPC message is configured with the given metadata
    void Send(Verb verb, std::unique_ptr<Payload> payload, Endpoint& endpoint, MessageMetadata metadata) override;

public: // distributed<> interface
    // iface: called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    // The method's returned future completes once all channels had a chance to complete a graceful shutdown
    seastar::future<> stop() override;

    // Should be called by user when all distributed objects have been created
    void Start() override;

private: // methods
    // utility method which ew use to obtain a connection(either existing or new) for the given endpoint
    seastar::lw_shared_ptr<TCPRPCChannel> _getOrMakeChannel(Endpoint& endpoint);

    // process a new channel creation
    void _handleNewChannel(seastar::lw_shared_ptr<TCPRPCChannel> chan);

    // Helper method to create an Endpoint from a socket address
    Endpoint _endpointFromAddress(seastar::socket_address addr);

private: // fields
    // the port we're listening on
    uint16_t _port;
    // we use this flag to signal exit
    bool _stopped;
    // our listening socket
    seastar::lw_shared_ptr<seastar::server_socket> _listen_socket;
    // the underlying TCP channels we're dealing with
    std::unordered_map<Endpoint, seastar::lw_shared_ptr<TCPRPCChannel>> _channels;

private: // not needed
    TCPRPCProtocol() = delete;
    TCPRPCProtocol(const TCPRPCProtocol& o) = delete;
    TCPRPCProtocol(TCPRPCProtocol&& o) = delete;
    TCPRPCProtocol &operator=(const TCPRPCProtocol& o) = delete;
    TCPRPCProtocol &operator=(TCPRPCProtocol&& o) = delete;

}; // class TCPRPCProtocol

} // namespace k2tx
