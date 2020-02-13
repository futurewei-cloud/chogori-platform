#include "Module.h"
#include <k2/dto/MessageVerbs.h>
namespace k2 {
namespace dto {
    static inline const Timestamp& max(const Timestamp& a, const Timestamp& b) {
        return (a.TEndTSECount() < b.TEndTSECount()) ? b : a;
    }
}
}

namespace k2 {

K23SIPartitionModule::K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition) : _cmeta(cmeta), _partition(partition) {
    K2INFO("ctor for cname=" << _cmeta.name <<", part=" << _partition);
    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse<Payload>>
    (dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
        return handleRead(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest<Payload>, dto::K23SIWriteResponse>
    (dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest<Payload>&& request) {
        return handleWrite(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIPushRequest, dto::K23SIPushResponse>
    (dto::Verbs::K23SI_PUSH, [this](dto::K23SIPushRequest&& request) {
        return handlePush(std::move(request));
    });
}

K23SIPartitionModule::~K23SIPartitionModule() {
    K2INFO("dtor for cname=" << _cmeta.name <<", part=" << _partition);
}

seastar::future<> K23SIPartitionModule::stop() {
    K2INFO("stop for cname=" << _cmeta.name << ", part=" << _partition);
    return seastar::make_ready_future();
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request) {
    // update read cache

    // read latest value

    (void)request;
    _readCache->insertInterval("k1", "k1", dto::Timestamp());

    return RPCResponse(Status::S501_Not_Implemented("Not implemented"), dto::K23SIReadResponse<Payload>());
}

seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::handleWrite(dto::K23SIWriteRequest<Payload>&& request) {
    (void)request;
    return RPCResponse(Status::S501_Not_Implemented("Not implemented"), dto::K23SIWriteResponse());
}

seastar::future<std::tuple<Status, dto::K23SIPushResponse>>
K23SIPartitionModule::handlePush(dto::K23SIPushRequest&& request) {
    (void)request;
    return RPCResponse(Status::S501_Not_Implemented("Not implemented"), dto::K23SIPushResponse());
}

} // ns k2
