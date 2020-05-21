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
#include <vector>
#include <unordered_map>

// k2
#include <k2/config/Config.h>
#include "IRPCProtocol.h"
#include "RPCProtocolFactory.h"
#include "RRDMARPCProtocol.h"
#include "RPCHeader.h"

namespace k2 {

// AutoRRDMARPCProtocol is a protocol which discovers a remote RRDMA endpoint using a TCP endpoint and
// the transport ListEndpoints verb.
// This protocol does not manage connections - it simply delegates to RRDMA if available from the remote end
// NB, the class is meant to be used as a distributed<> container
class AutoRRDMARPCProtocol: public IRPCProtocol {
public: // types
    // Convenience builder to allow the stack to be used on all cores
    static RPCProtocolFactory::BuilderFunc_t builder(VirtualNetworkStack::Dist_t& vnet, RPCProtocolFactory::Dist_t& rrdmaProto);


    // The official protocol name supported for communications over AutoRRDMARPC channels
    static const String proto;

public: // lifecycle
    // Construct the protocol with a protocol which supports RRDMA
    AutoRRDMARPCProtocol(VirtualNetworkStack::Dist_t& vnet, RPCProtocolFactory::Dist_t& rrdmaProto);

    // Destructor
    virtual ~AutoRRDMARPCProtocol();

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
    void send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& autoEndpoint, MessageMetadata metadata) override;

    // Returns the endpoint where this protocol accepts incoming connections.
    seastar::lw_shared_ptr<TXEndpoint> getServerEndpoint() override;

public: // distributed<> interface
    // iface: called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    // The method's returned future completes once all channels had a chance to complete a graceful shutdown
    seastar::future<> stop() override;

    // Should be called by user when all distributed objects have been created
    void start() override;

private: // fields
    // maps auto-rrdma to rrdma endpoint and pending writes for that endpoint
    typedef std::vector<std::tuple<Verb, std::unique_ptr<Payload>, MessageMetadata>> _Buffer;
    std::unordered_map<TXEndpoint, std::tuple<std::unique_ptr<TXEndpoint>, _Buffer>> _endpoints;

    // the rrdma protocol
    seastar::shared_ptr<IRPCProtocol> _rrdmaProto;

    // used to tell if we have any pending discovery requests
    seastar::future<> _pendingDiscovery = seastar::make_ready_future();

    ConfigDuration _listTimeout{"auto_rrdma_list_endpoints_timeout", 10s};

    bool _stopped = true;

    std::unique_ptr<TXEndpoint> _getTCPEndpoint(const String& autoURL);

private: // not needed
    AutoRRDMARPCProtocol() = delete;
    AutoRRDMARPCProtocol(const AutoRRDMARPCProtocol& o) = delete;
    AutoRRDMARPCProtocol(AutoRRDMARPCProtocol&& o) = delete;
    AutoRRDMARPCProtocol &operator=(const AutoRRDMARPCProtocol& o) = delete;
    AutoRRDMARPCProtocol &operator=(AutoRRDMARPCProtocol&& o) = delete;

}; // class AutoRRDMARPCProtocol

} // namespace k2
