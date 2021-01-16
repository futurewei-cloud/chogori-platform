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
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
// k2
#include "IRPCProtocol.h"
#include "VirtualNetworkStack.h"
#include "RPCProtocolFactory.h"
#include "TCPRPCChannel.h"
#include "RPCHeader.h"

namespace k2 {

// TCPRPCProtocol is a protocol which use the currently configured TCP stack, with responsibility to:
// - listen for incoming TCP connections
// - create outgoing TCP connections when asked to send messages
// - receive incoming messages and pass them on to the message observer for the protocol
// NB, the class is meant to be used as a distributed<> container
class TCPRPCProtocol: public IRPCProtocol {
public: // types
    // Convenience builder which does not create a listener (client-mode only)
    static RPCProtocolFactory::BuilderFunc_t builder(VirtualNetworkStack::Dist_t& vnet);

    // Convenience builder which opens a listener on the same port across all cores
    static RPCProtocolFactory::BuilderFunc_t builder(VirtualNetworkStack::Dist_t& vnet, uint16_t port);

    // Allow building of protocols with an address provider.
    static RPCProtocolFactory::BuilderFunc_t builder(VirtualNetworkStack::Dist_t& vnet, IAddressProvider& addrProvider);

    // The official protocol name supported for communications over TCPRPC channels
    static inline const String proto{"tcp+k2rpc"};

   public:  // lifecycle
    // Construct the protocol with a vnet which supports TCP and listens on the given address
    TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet, SocketAddress addr);

    // Construct the protocol with a vnet which supports TCP and no ability to accept incoming connections
    TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet);

    // Destructor
    virtual ~TCPRPCProtocol();

public: // API
    // This method creates an endpoint for a given URL. The endpoint is needed in order to
    // 1. obtain protocol-specific payloads
    // 2. send messages.
    // returns blank pointer if we failed to parse the url or if the protocol is not supported
    std::unique_ptr<TXEndpoint> getTXEndpoint(String url) override;

    // Invokes the remote rpc for the given verb with the given payload. This is an asyncronous API. No guarantees
    // are made on the delivery of the payload after the call returns.
    // This is a lower-level API which is useful for sending messages that do not expect replies.
    // The RPC message is configured with the given metadata
    void send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata metadata) override;

    // Returns the endpoint where this protocol accepts incoming connections.
    seastar::lw_shared_ptr<TXEndpoint> getServerEndpoint() override;

public: // distributed<> interface
    // iface: called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    // The method's returned future completes once all channels had a chance to complete a graceful shutdown
    seastar::future<> stop() override;

    // Should be called by user when all distributed objects have been created
    void start() override;

private: // methods
    // utility method which ew use to obtain a connection(either existing or new) for the given endpoint
    seastar::lw_shared_ptr<TCPRPCChannel> _getOrMakeChannel(TXEndpoint& endpoint);

    // process a new channel creation
    seastar::lw_shared_ptr<TCPRPCChannel>
    _handleNewChannel(seastar::future<seastar::connected_socket> futureSocket, const TXEndpoint& endpoint);

    // Helper method to create an TXEndpoint from a socket address
    TXEndpoint _endpointFromAddress(SocketAddress addr);

private: // fields
    // the address we're listening on
    SocketAddress _addr;

    // the endpoint version of the address we're listening on
    seastar::lw_shared_ptr<TXEndpoint> _svrEndpoint;

    // we use this flag to signal exit
    bool _stopped;
    // our listening socket
    seastar::lw_shared_ptr<seastar::server_socket> _listen_socket;
    seastar::future<> _listenerClosed = seastar::make_ready_future();

    // the underlying TCP channels we're dealing with
    std::unordered_map<TXEndpoint, seastar::lw_shared_ptr<TCPRPCChannel>> _channels;

private: // not needed
    TCPRPCProtocol() = delete;
    TCPRPCProtocol(const TCPRPCProtocol& o) = delete;
    TCPRPCProtocol(TCPRPCProtocol&& o) = delete;
    TCPRPCProtocol &operator=(const TCPRPCProtocol& o) = delete;
    TCPRPCProtocol &operator=(TCPRPCProtocol&& o) = delete;

}; // class TCPRPCProtocol

} // namespace k2
