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
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include "Config.h"
#include "Log.h"

namespace k2 {
class Persistence {
public:
    Persistence();
    template<typename ValueType>
    seastar::future<> makeCall(const ValueType& val, FastDeadline deadline) {
        if (_remoteEndpoint) {
            auto payload = _remoteEndpoint->newPayload();
            payload->write(val);
            dto::K23SI_PersistenceRequest<Payload> request{};
            request.value.val = std::move(*payload);
            K2LOG_D(log::skvsvr, "making persistence call to endpoint: {}, with deadline={}", _remoteEndpoint->getURL(), deadline.getRemaining());
            return seastar::do_with(std::move(request), [this, deadline] (auto& request) {
                return RPC().callRPC<dto::K23SI_PersistenceRequest<Payload>, dto::K23SI_PersistenceResponse>
                    (dto::Verbs::K23SI_Persist, request, *_remoteEndpoint, deadline.getRemaining()).discard_result();
            });
        }
        else {
            return seastar::make_exception_future(std::runtime_error("Persistence not availabe"));
        }
    }
private:
    std::unique_ptr<TXEndpoint> _remoteEndpoint;
    K23SIConfig _config;
};
}
