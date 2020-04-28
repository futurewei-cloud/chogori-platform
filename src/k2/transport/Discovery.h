#pragma once

// third-party

#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

#include "TCPRPCProtocol.h"
#include "TXEndpoint.h"
#include "RPCDispatcher.h"  // for RPC
#include "RPCTypes.h"
#include "RRDMARPCProtocol.h"

namespace k2 {
class Discovery {
public :  // application lifespan
    Discovery();
    ~Discovery();

    // This method determines the endpoint for the best match between this node's capabilities and the given URLs.
    template<typename StringContainer>
    static std::unique_ptr<TXEndpoint> selectBestEndpoint(const StringContainer& urls) {
        std::vector<std::unique_ptr<TXEndpoint>> eps;

        for(auto& url: urls) {
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
    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();
};  // class Discovery

} // namespace k2
