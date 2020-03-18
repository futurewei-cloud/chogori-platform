#include "Persistence.h"
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>

namespace k2 {

Persistence::Persistence() {
    //TODO discover RDMA endpoint
    _remoteEndpoint = RPC().getTXEndpoint(_config.persistenceEndpoint());
}

seastar::future<> Persistence::makeCall(FastDeadline deadline) {
    //TODO fix all users to use persistence appropriately. For now, we just mock a remote network call
    dto::K23SI_PersistenceRequest request{};
    if (_remoteEndpoint) {
        return RPC().callRPC<dto::K23SI_PersistenceRequest, dto::K23SI_PersistenceResponse>
            (dto::Verbs::K23SI_Persist, request, *_remoteEndpoint, deadline.getRemaining()).discard_result();
    }
    else {
        return seastar::make_exception_future(std::runtime_error("Persistence not availabe"));
    }
}

}
