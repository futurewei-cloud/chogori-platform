//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
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
const String AutoRRDMARPCProtocol::proto("auto-rrdma+k2rpc");

AutoRRDMARPCProtocol::AutoRRDMARPCProtocol(VirtualNetworkStack::Dist_t& vnet, RPCProtocolFactory::Dist_t& rrdmaProto):
    IRPCProtocol(vnet, proto),
    _rrdmaProto(rrdmaProto.local().instance()) {
    K2DEBUG("ctor");
}

AutoRRDMARPCProtocol::~AutoRRDMARPCProtocol() {
    K2DEBUG("dtor");
}

void AutoRRDMARPCProtocol::start() {
    K2DEBUG("start");
    _stopped = false;
}

RPCProtocolFactory::BuilderFunc_t AutoRRDMARPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, RPCProtocolFactory::Dist_t& rrdmaProto) {
    K2DEBUG("builder creating");
    return [&vnet, &rrdmaProto]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2DEBUG("builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<AutoRRDMARPCProtocol>(vnet, rrdmaProto));
    };
}

seastar::future<> AutoRRDMARPCProtocol::stop() {
    K2DEBUG("stop");
    if (!_stopped) {
        _stopped = true;
        return std::move(_pendingDiscovery);
    }
    return seastar::make_ready_future();
}

std::unique_ptr<TXEndpoint> AutoRRDMARPCProtocol::getTXEndpoint(String url) {
    if (_stopped) {
        K2WARN("Auto RRDMA proto is stopped - cannot vend endpoint");
        return nullptr;
    }
    auto ep = TXEndpoint::fromURL(url, _vnet.local().getRRDMAAllocator());
    if (!ep || ep->getProtocol() != proto) {
        K2WARN("Cannot construct non-`" << proto << "` endpoint");
        return nullptr;
    }
    return std::move(ep);
}

seastar::lw_shared_ptr<TXEndpoint> AutoRRDMARPCProtocol::getServerEndpoint() {
    return nullptr;
}

void AutoRRDMARPCProtocol::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& autoEndpoint, MessageMetadata metadata) {
    if (_stopped) {
        K2WARN("Dropping message since we're stopped: verb=" << verb << ", url=" << autoEndpoint.getURL());
        return;
    }
    auto it = _endpoints.find(autoEndpoint);
    if (it == _endpoints.end()) {
        // construct default tuple with empty endpoint and a list
        auto& [ep, pending] = _endpoints[autoEndpoint];
        K2DEBUG("queueing up against new ep: " << autoEndpoint);
        pending.push_back({verb, std::move(payload), std::move(metadata)});
        auto tcpEp = _getTCPEndpoint(autoEndpoint.getURL());
        ListEndpointsRequest request{};
        auto newDiscovery = RPC().callRPC<ListEndpointsRequest, ListEndpointsResponse>
            (InternalVerbs::LIST_ENDPOINTS, request, *tcpEp, _listTimeout())
            .then([this, &ep, &pending, it] (auto&& responseTup) mutable {
                auto& [status, response] = responseTup;
                if (!status.is2xxOK()) {
                    K2WARN("received bad status: " << status);
                    throw(std::runtime_error("received bad status"));
                }
                // set the unique_ptr endpoint to the rdma endpoint from the list we just got
                for (auto& availableEp: response.endpoints) {
                    if (availableEp.find(RRDMARPCProtocol::proto) == 0) {
                        ep = _rrdmaProto->getTXEndpoint(std::move(availableEp));
                        break;
                    }
                }
                if (!ep) {
                    K2WARN("Unable to find RRDMA endpoint from " << it->first.getURL());
                    throw(std::runtime_error("unable to find an RDMA endpoint in response"));
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
            .handle_exception([this, it](auto exc) {
                K2WARN_EXC("unable to find an RRDMA endpoint", exc);
                _endpoints.erase(it);
            });
        _pendingDiscovery = seastar::when_all(std::move(_pendingDiscovery), std::move(newDiscovery)).discard_result();
    }
    else {
        auto& [ep, pending] = it->second;
        if (!pending.empty()) {
            K2DEBUG("queueing up against pending ep: " << endpoint);
            pending.push_back({verb, std::move(payload), std::move(metadata)});
        }
        else {
            K2DEBUG("Sending via RRDMA ep: " << ep);
            _rrdmaProto->send(verb, std::move(payload), *ep, std::move(metadata));
        }
    }
}

std::unique_ptr<TXEndpoint> AutoRRDMARPCProtocol::_getTCPEndpoint(const String& autoURL) {
    String tcpURL = "tcp" + autoURL.substr(autoURL.find("+"));
    return RPC().getTXEndpoint(std::move(tcpURL));
}

} // namespace k2
