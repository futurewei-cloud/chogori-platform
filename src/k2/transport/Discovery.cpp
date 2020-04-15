#include "Discovery.h"

#include <k2/common/Log.h>

#include "AutoRRDMARPCProtocol.h"
#include "DiscoveryDTO.h"
#include "Status.h"

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
        return RPCResponse(Statuses::S200_OK("list endpoints success"), std::move(response));
    });

    return seastar::make_ready_future();
}

} // namespace k2
