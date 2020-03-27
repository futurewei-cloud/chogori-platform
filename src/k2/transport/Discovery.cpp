#include "Discovery.h"

#include <k2/common/Log.h>

#include "AutoRRDMARPCProtocol.h"
#include "DiscoveryDTO.h"
#include "RPCDispatcher.h"  // for RPC
#include "RPCTypes.h"
#include "RRDMARPCProtocol.h"
#include "Status.h"
#include "TCPRPCProtocol.h"

namespace k2 {

Discovery::Discovery() {
    K2INFO("ctor");
}

Discovery::~Discovery() {
    K2INFO("dtor");
}

seastar::future<> Discovery::stop() {
    K2INFO("stop");
    return seastar::make_ready_future();
}

seastar::future<> Discovery::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<ListEndpointsRequest, ListEndpointsResponse>(InternalVerbs::LIST_ENDPOINTS,
    [this](ListEndpointsRequest&&) {
        ListEndpointsResponse response{};
        for (auto& serverEndpoint : RPC().getServerEndpoints()) {
            if (serverEndpoint) {
                response.endpoints.push_back(serverEndpoint->getURL());
            }
        }
        return RPCResponse(Status::S200_OK(), std::move(response));
    });

    return seastar::make_ready_future();
}

std::unique_ptr<TXEndpoint> Discovery::selectBestEndpoint(const std::vector<String>& urls) {
    std::vector<std::unique_ptr<TXEndpoint>> eps;

    for (auto& url : urls) {
        auto ep = RPC().getTXEndpoint(std::move(url));
        if (ep) {
            eps.push_back(std::move(ep));
        }
    }
    // look for rdma
    if (seastar::engine()._rdma_stack) {
        for (auto& ep: eps) {
            if (ep->getProtocol() == RRDMARPCProtocol::proto) {
                return std::move(ep);
            }
        }
    }

    // either we don't support rdma, or remote end doesn't. Look for TCP
    for (auto& ep : eps) {
        if (ep->getProtocol() == TCPRPCProtocol::proto) {
            return std::move(ep);
        }
    }

    // no match
    return nullptr;
}

} // namespace k2
