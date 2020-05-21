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

#include "PersistenceService.h"
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/K23SI.h>

#include <k2/common/Log.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC

namespace k2 {

PersistenceService::PersistenceService() {
    K2INFO("ctor");
}

PersistenceService::~PersistenceService() {
    K2INFO("dtor");
}

seastar::future<> PersistenceService::gracefulStop() {
    K2INFO("stop");
    return seastar::make_ready_future();
}

seastar::future<> PersistenceService::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<dto::K23SI_PersistenceRequest<Payload>, dto::K23SI_PersistenceResponse>
    (dto::Verbs::K23SI_Persist, [this](dto::K23SI_PersistenceRequest<Payload>&& request) {
        (void) request;
        return RPCResponse(Statuses::S200_OK("persistence success"), dto::K23SI_PersistenceResponse{});
    });

    return seastar::make_ready_future();
}

} // namespace k2
