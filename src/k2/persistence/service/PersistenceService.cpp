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
