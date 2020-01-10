#include "CPOService.h"

#include <k2/common/Log.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/dto/ControlPlaneOracle.h> // our DTO
#include <k2/dto/MessageVerbs.h> // our DTO

namespace k2 {

CPOService::CPOService(DistGetter distGetter) : _dist(distGetter) {
    K2INFO("ctor");
}

CPOService::~CPOService() {
    K2INFO("dtor");
}

seastar::future<> CPOService::stop() {
    K2INFO("stop");
    return seastar::make_ready_future<>();
}

seastar::future<> CPOService::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<dto::CollectionCreateRequest, dto::CollectionCreateRequest>(dto::Verbs::CPO_COLLECTION_CREATE, [this](dto::CollectionCreateRequest&& request) {
        K2INFO("Received collection create request for " << request.metadata.name);
        return std::make_tuple(Status::Ok, dto::CollectionCreateRequest());
    });
    return seastar::make_ready_future<>();
}

} // namespace k2
