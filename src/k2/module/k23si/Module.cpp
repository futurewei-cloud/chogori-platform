#include "Module.h"
#include <k2/dto/MessageVerbs.h>
#include <k2/appbase/AppEssentials.h>
namespace k2 {
namespace dto {
    // we want the read cache to determine ordering based on certain comparison so that we have some stable
    // ordering even across different nodes and TSOs
    const Timestamp& max(const Timestamp& a, const Timestamp& b) {
        return a.compareCertain(b) == Timestamp::GT ? b : a;
    }
} // ns dto

K23SIPartitionModule::K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition) : _cmeta(std::move(cmeta)), _partition(std::move(partition)) {
    K2INFO("ctor for cname=" << _cmeta.name <<", part=" << _partition);
}

seastar::future<> K23SIPartitionModule::start() {
    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse<Payload>>(dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
        if (!_validateRequestPartition(request)) {
            // tell client their collection partition is gone
            return RPCResponse(Status::S410_Gone(),dto::K23SIReadResponse<Payload>());
        }
        return handleRead(std::move(request), dto::K23SI_MTR_ZERO);
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest<Payload>, dto::K23SIWriteResponse>
    (dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest<Payload>&& request) {
        return handleWrite(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
    (dto::Verbs::K23SI_TXN_PUSH, [this](dto::K23SITxnPushRequest&& request) {
        return handleTxnPush(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
    (dto::Verbs::K23SI_TXN_END, [this](dto::K23SITxnEndRequest&& request) {
        return handleTxnEnd(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
    (dto::Verbs::K23SI_TXN_FINALIZE, [this](dto::K23SITxnFinalizeRequest&& request) {
        return handleTxnFinalize(std::move(request));
    });

    // todo call TSO to get a timestamp
    return seastar::make_ready_future<dto::Timestamp>(dto::Timestamp())
        .then([this](dto::Timestamp watermark) {
            ConfigVar<uint64_t> readCacheSize("k23si_read_cache_size", 10000);
            _readCache = std::make_unique<ReadCache<dto::Key, dto::Timestamp>>(watermark, readCacheSize());
            return seastar::make_ready_future();
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
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request, dto::K23SI_MTR sitMTR) {
    // update the read cache to lock out any future writers which may attempt to modify the key range
    // before this read's timestamp
    if (sitMTR == dto::K23SI_MTR_ZERO) {
        _readCache->insertInterval(request.key, request.key, request.mtr.timestamp);
    }

    auto fiter = _indexer.find(request.key);
    if (fiter == _indexer.end()) {
        return _makeReadResponse(nullptr);
    }
    auto& versions = fiter->second;
    auto viter = versions.begin();
    // position the version iterator at the version we should be returning
    while(viter != versions.end() && request.mtr.timestamp.compareCertain(viter->timestamp) < 0) {
        ++viter;
    }
    if (viter == versions.end()) {
        // we either had no versions, or all versions were newer than the requested timestamp
        return _makeReadResponse(nullptr);
    }

    // happy case: either committed, or txn is reading its own write
    if (viter->status == DataRecord::Committed || viter->mtr == request.mtr) {
        return _makeReadResponse(&(*viter));
    }
    // record is still pending and isn't from same transaction.

    if (sitMTR == dto::K23SI_MTR_ZERO) {
        sitMTR = viter->mtr;
        // this is a fresh read finding a WI. have to do a push
        return _doPush(viter->trh, sitMTR, request.mtr)
            .then([this, sitMTR, request=std::move(request)](auto winnerMTR) mutable {
                if (winnerMTR == sitMTR) {
                    // sitting transaction won. Abort the incoming request
                    auto resp = dto::K23SIReadResponse<Payload>();
                    resp.abortPriority = sitMTR.priority;
                    return RPCResponse(Status::S409_Conflict(), std::move(resp));
                }
                // incoming request won. re-run read logic to get
                return handleRead(std::move(request), sitMTR);
            });
    }
    // this is a read after a push and we still find a WI. This WI must be the exact same one we pushed against,
    // or else our read cache was not used correctly in code.
    K2ASSERT(sitMTR == viter->mtr, "bug in code: found WI after push");
    _queueWICleanup(std::move(*viter));
    ++viter;
    return _makeReadResponse(viter == versions.end() ? nullptr : &(*viter));
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
K23SIPartitionModule::_makeReadResponse(DataRecord* rec) const {
    if (rec == nullptr || rec->isTombstone) {
        return RPCResponse(Status::S404_Not_Found(), dto::K23SIReadResponse<Payload>());
    }

    auto response = dto::K23SIReadResponse<Payload>();
    response.key = rec->key;
    response.value.val = rec->value.val.share();
    return RPCResponse(Status::S200_OK(""), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::handleWrite(dto::K23SIWriteRequest<Payload>&& request) {
    (void)request;
    // NB if we try to place a WI over a committed value with same ts.end (even if from different TSO), reject the
    // incoming write in order to avoid weird read-my-write problem for in-progress transactions
    return RPCResponse(Status::S501_Not_Implemented("Not implemented"), dto::K23SIWriteResponse());
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
K23SIPartitionModule::handleTxnPush(dto::K23SITxnPushRequest&& request) {
    (void)request;
    return RPCResponse(Status::S501_Not_Implemented("Not implemented"), dto::K23SITxnPushResponse());
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
K23SIPartitionModule::handleTxnEnd(dto::K23SITxnEndRequest&& request) {
    (void)request;
    return RPCResponse(Status::S501_Not_Implemented("Not implemented"), dto::K23SITxnEndResponse());
}

seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
K23SIPartitionModule::handleTxnFinalize(dto::K23SITxnFinalizeRequest&& request) {
    (void)request;
    return RPCResponse(Status::S501_Not_Implemented("Not implemented"), dto::K23SITxnFinalizeResponse());
}

seastar::future<dto::K23SI_MTR>
K23SIPartitionModule::_doPush(dto::K23SI_TRH_ID trh, dto::K23SI_MTR sitMTR, dto::K23SI_MTR pushMTR) {
    (void) trh;
    (void) pushMTR;
    return seastar::make_ready_future<dto::K23SI_MTR>(std::move(sitMTR));
}

void K23SIPartitionModule::_queueWICleanup(DataRecord rec) {
    (void) rec; // just drop the record
}

} // ns k2
