#pragma once
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include "Config.h"

namespace k2 {
class Persistence {
public:
    Persistence();
    template<typename ValueType>
    seastar::future<> makeCall(const ValueType& val, FastDeadline deadline) {
        return seastar::do_with(dto::K23SI_PersistenceRequest<Payload>{}, [&val, this, deadline] (auto& request) {
            if (_remoteEndpoint) {
                auto payload = _remoteEndpoint->newPayload();
                payload->write(val);
                request.value.val = std::move(*payload);
                K2DEBUG("making persistence call to endpoint: " << _remoteEndpoint->getURL() << ", with deadline=" << deadline.getRemaining());

                return RPC().callRPC<dto::K23SI_PersistenceRequest<Payload>, dto::K23SI_PersistenceResponse>
                    (dto::Verbs::K23SI_Persist, request, *_remoteEndpoint, deadline.getRemaining()).discard_result();
            }
            else {
                return seastar::make_exception_future(std::runtime_error("Persistence not availabe"));
            }
        });
    }
private:
    std::unique_ptr<TXEndpoint> _remoteEndpoint;
    K23SIConfig _config;
};
}
