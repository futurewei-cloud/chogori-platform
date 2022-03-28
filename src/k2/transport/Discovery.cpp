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

#include "Discovery.h"

#include <k2/common/Log.h>

#include "AutoRRDMARPCProtocol.h"
#include "DiscoveryDTO.h"
#include <k2/dto/shared/Status.h>

namespace k2 {

Discovery::Discovery() {
    K2LOG_I(log::tx, "ctor");
}

Discovery::~Discovery() {
    K2LOG_I(log::tx, "dtor");
}

seastar::future<> Discovery::gracefulStop() {
    K2LOG_I(log::tx, "graceful stop");
    return seastar::make_ready_future();
}

seastar::future<> Discovery::start() {
    K2LOG_I(log::tx, "Registering message handlers");
    RPC().registerRPCObserver<ListEndpointsRequest, ListEndpointsResponse>(InternalVerbs::LIST_ENDPOINTS,
    [this](ListEndpointsRequest&&) {
        ListEndpointsResponse response{};
        for (auto& serverEndpoint : RPC().getServerEndpoints()) {
            if (serverEndpoint) {
                response.endpoints.push_back(serverEndpoint->url);
            }
        }
        return RPCResponse(Statuses::S200_OK("list endpoints success"), std::move(response));
    });

    return seastar::make_ready_future();
}

} // namespace k2
