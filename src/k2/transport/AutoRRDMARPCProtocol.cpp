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

#include "AutoRRDMARPCProtocol.h"

#include "DiscoveryDTO.h"
#include "Discovery.h"
#include "RPCDispatcher.h"
#include "RPCTypes.h"

// third-party
#include <seastar/core/future-util.hh>

//k2
#include <k2/common/Log.h>

namespace k2 {

AutoRRDMARPCProtocol::AutoRRDMARPCProtocol(VirtualNetworkStack::Dist_t& vnet, RPCProtocolFactory::Dist_t& rrdmaProto):
    IRPCProtocol(vnet, proto),
    _rrdmaProto(rrdmaProto.local().instance()) {
    K2LOG_D(log::tx, "ctor");
}

AutoRRDMARPCProtocol::~AutoRRDMARPCProtocol() {
    K2LOG_D(log::tx, "dtor");
}

void AutoRRDMARPCProtocol::start() {
    K2LOG_D(log::tx, "start");
    _stopped = false;
}

RPCProtocolFactory::BuilderFunc_t AutoRRDMARPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, RPCProtocolFactory::Dist_t& rrdmaProto) {
    K2LOG_D(log::tx, "builder creating");
    return [&vnet, &rrdmaProto]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2LOG_D(log::tx, "builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<AutoRRDMARPCProtocol>(vnet, rrdmaProto));
    };
}

seastar::future<> AutoRRDMARPCProtocol::stop() {
    K2LOG_D(log::tx, "stop");
    if (!_stopped) {
        _stopped = true;
        return std::move(_pendingDiscovery);
    }
    return seastar::make_ready_future();
}

std::unique_ptr<TXEndpoint> AutoRRDMARPCProtocol::getTXEndpoint(String url) {
    if (_stopped) {
        K2LOG_W(log::tx, "Auto RRDMA proto is stopped - cannot vend endpoint");
        return nullptr;
    }
    auto ep = TXEndpoint::fromURL(url, _vnet.local().getRRDMAAllocator());
    if (!ep || ep->protocol != proto) {
        K2LOG_W(log::tx, "Cannot construct non-`{}` endpoint", proto);
        return nullptr;
    }
    return ep;
}

seastar::lw_shared_ptr<TXEndpoint> AutoRRDMARPCProtocol::getServerEndpoint() {
    return nullptr;
}

void AutoRRDMARPCProtocol::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& autoEndpoint, MessageMetadata metadata) {
    if (_stopped) {
        K2LOG_W(log::tx, "Dropping message since we're stopped: verb={}, url={}", int(verb), autoEndpoint.url);
        return;
    }
    auto& [ep, pending] = _endpoints[autoEndpoint]; // create or get the existing
    if (!ep || pending.size() > 0) {
        // queue up against pending request
        K2LOG_D(log::tx, "queueing up against new ep: {}", autoEndpoint.url);
        pending.push_back({verb, std::move(payload), std::move(metadata)});
    }
    if (!ep) {
        // we haven't resolved this yet. We should start a resolution only for the first-ever request
        if (pending.size() > 1) {
            K2LOG_D(log::tx, "resolution request already in progress. Pending size={}", pending.size());
            return;
        }
        auto tcpEp = _getTCPEndpoint(autoEndpoint.url);
        ListEndpointsRequest request{};
        auto newDiscovery = RPC().callRPC<ListEndpointsRequest, ListEndpointsResponse>
            (InternalVerbs::LIST_ENDPOINTS, request, *tcpEp, _listTimeout())
            .then([this, &ep, &pending] (auto&& responseTup) mutable {
                auto& [status, response] = responseTup;
                if (!status.is2xxOK()) {
                    K2LOG_W(log::tx, "received bad status: {}", status);
                    return seastar::make_exception_future(std::runtime_error("received bad status"));
                }
                // set the unique_ptr endpoint to the rdma endpoint from the list we just got
                for (auto& availableEp: response.endpoints) {
                    if (availableEp.find(RRDMARPCProtocol::proto) == 0) {
                        ep = _rrdmaProto->getTXEndpoint(std::move(availableEp));
                        break;
                    }
                }
                if (!ep) {
                    K2LOG_W(log::tx, "Unable to find RRDMA endpoint");
                    return seastar::make_exception_future(std::runtime_error("unable to find an RDMA endpoint in response"));
                }
                else if (!_stopped) {
                    for(auto& tup: pending) {
                        auto& [verb, payload, metadata] = tup;
                        _rrdmaProto->send(verb, std::move(payload), *ep, std::move(metadata));
                    }
                    pending.resize(0);
                }
                return seastar::make_ready_future();
            })
            .handle_exception([this, autoEndpoint](auto exc) {
                K2LOG_W_EXC(log::tx, exc, "unable to find an RRDMA endpoint");
                _endpoints.erase(autoEndpoint);
            });
        _pendingDiscovery = seastar::when_all_succeed(std::move(_pendingDiscovery), std::move(newDiscovery)).discard_result();
    }
    else {
        K2LOG_D(log::tx, "Sending via RRDMA ep: {}", ep->url);
        _rrdmaProto->send(verb, std::move(payload), *ep, std::move(metadata));
    }
}

std::unique_ptr<TXEndpoint> AutoRRDMARPCProtocol::_getTCPEndpoint(const String& autoURL) {
    String tcpURL = "tcp" + autoURL.substr(autoURL.find("+"));
    return RPC().getTXEndpoint(std::move(tcpURL));
}

} // namespace k2
