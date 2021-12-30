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

// third-party

#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

#include "TCPRPCProtocol.h"
#include "TXEndpoint.h"
#include "RPCDispatcher.h"  // for RPC
#include "RPCTypes.h"
#include "RRDMARPCProtocol.h"
#include "Log.h"

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
                if (ep->protocol == RRDMARPCProtocol::proto) {
                    return std::move(ep);
                }
            }
        }

        // either we don't support rdma, or remote end doesn't. Look for TCP
        for (auto& ep : eps) {
            if (ep->protocol == TCPRPCProtocol::proto) {
                return std::move(ep);
            }
        }

        // no match
        return nullptr;
    }

    template<typename StringContainer>
    static String selectBestEndpointString(const StringContainer& urls) {
        bool rdma = seastar::engine()._rdma_stack != nullptr;
        String selectedEP = "";

        for (const String& ep : urls) {
            if (ep.find(RRDMARPCProtocol::proto) != String::npos && rdma) {
                selectedEP = ep;
            }
            else if (ep.find(TCPRPCProtocol::proto) != String::npos && !rdma) {
                selectedEP = ep;
            }
        }

        return selectedEP;
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();
};  // class Discovery

} // namespace k2
