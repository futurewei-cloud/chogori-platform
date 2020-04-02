#include "Module.h"
#include <k2/dto/MessageVerbs.h>
#include <k2/appbase/AppEssentials.h>
namespace k2 {
namespace dto {
    // we want the read cache to determine ordering based on certain comparison so that we have some stable
    // ordering even across different nodes and TSOs
    const Timestamp& max(const Timestamp& a, const Timestamp& b) {
        return a.compareCertain(b) == Timestamp::LT ? b : a;
    }
} // ns dto


// get timeNow Timestamp from TSO
seastar::future<dto::Timestamp> getTimeNow() {
    // TODO call TSO service with timeout and retry logic
    auto nsecsSinceEpoch = nsec(CachedSteadyClock::now(true).time_since_epoch()).count();
    return seastar::make_ready_future<dto::Timestamp>(dto::Timestamp(nsecsSinceEpoch, 1550647543, 1000));
}


K23SIPartitionModule::K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition) :
    _cmeta(std::move(cmeta)),
    _partition(std::move(partition)),
    _retentionUpdateTimer([this] {
        K2DEBUG("Partition: " << _partition << ", refreshing retention timestamp");
        _retentionRefresh = _retentionRefresh.then([]{
            return getTimeNow();
        })
        .then([this](dto::Timestamp&& ts) {
            // set the retention timestamp (the time of the oldest entry we should keep)
            _retentionTimestamp = ts - _cmeta.retentionPeriod;
            _txnMgr.updateRetentionTimestamp(_retentionTimestamp);
        })
        .finally([this]{
            _retentionUpdateTimer.arm(_config.retentionTimestampUpdateInterval());
        });
    }),
    _cpo(_config.cpoEndpoint()) {
    K2INFO("ctor for cname=" << _cmeta.name <<", part=" << _partition);
}

seastar::future<> K23SIPartitionModule::start() {
    K2DEBUG("Starting for partition: " << _partition);
    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse<Payload>>(dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
        K2DEBUG("Partition: " << _partition << ", received read for key " << request.key);
        if (!_validateRequestPartition(request)) {
            // tell client their collection partition is gone
            return RPCResponse(dto::K23SIStatus::RefreshCollection(), dto::K23SIReadResponse<Payload>());
        }
        if (!_validateRetentionWindow(request)) {
            // the request is outside the retention window
            return RPCResponse(dto::K23SIStatus::AbortRequestTooOld(), dto::K23SIReadResponse<Payload>());
        }
        return handleRead(std::move(request), dto::K23SI_MTR_ZERO, FastDeadline(_config.readTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest<Payload>, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest<Payload>&& request) {
        return handleWrite(std::move(request), dto::K23SI_MTR_ZERO, FastDeadline(_config.writeTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
    (dto::Verbs::K23SI_TXN_PUSH, [this](dto::K23SITxnPushRequest&& request) {
        return handleTxnPush(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
    (dto::Verbs::K23SI_TXN_END, [this](dto::K23SITxnEndRequest&& request) {
        return handleTxnEnd(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
    (dto::Verbs::K23SI_TXN_HEARTBEAT, [this](dto::K23SITxnHeartbeatRequest&& request) {
        return handleTxnHeartbeat(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
    (dto::Verbs::K23SI_TXN_FINALIZE, [this](dto::K23SITxnFinalizeRequest&& request) {
        return handleTxnFinalize(std::move(request));
    });

    if (_cmeta.retentionPeriod < _config.minimumRetentionPeriod()) {
        K2WARN("Requested retention(" << _cmeta.retentionPeriod << ") is lower than minimum("
                                      << _config.minimumRetentionPeriod() << "). Extending retention to minimum");
        _cmeta.retentionPeriod = _config.minimumRetentionPeriod();
    }

    // todo call TSO to get a timestamp
    return getTimeNow()
        .then([this](dto::Timestamp&& watermark) {
            _retentionTimestamp = watermark - _cmeta.retentionPeriod;
            _readCache = std::make_unique<ReadCache<dto::Key, dto::Timestamp>>(watermark, _config.readCacheSize());
            _retentionUpdateTimer.arm(_config.retentionTimestampUpdateInterval());
            return seastar::when_all(_recovery(), _txnMgr.start(_cmeta.name, _retentionTimestamp, _cmeta.heartbeatDeadline)).discard_result();
        });
}

K23SIPartitionModule::~K23SIPartitionModule() {
    K2INFO("dtor for cname=" << _cmeta.name <<", part=" << _partition);
}

seastar::future<> K23SIPartitionModule::_recovery() {
    //TODO perform recovery
    K2DEBUG("Partition: " << _partition << ", recovery");
    return _persistence.makeCall(FastDeadline(10s));
}

seastar::future<> K23SIPartitionModule::stop() {
    K2INFO("stop for cname=" << _cmeta.name << ", part=" << _partition);
    _retentionUpdateTimer.cancel();
    return seastar::when_all(std::move(_retentionRefresh), _txnMgr.stop()).discard_result();
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
_makeReadOK(DataRecord* rec) {
    if (rec == nullptr || rec->isTombstone) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound(), dto::K23SIReadResponse<Payload>{});
    }

    auto response = dto::K23SIReadResponse<Payload>();
    response.value.val = rec->value.val.share();
    return RPCResponse(dto::K23SIStatus::OK(), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline) {
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection(), dto::K23SIReadResponse<Payload>{});
    }
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld(), dto::K23SIReadResponse<Payload>{});
    }
    // sitMTR will be ZERO for original requests or non-zero for post-PUSH reads
    // update the read cache to lock out any future writers which may attempt to modify the key range
    // before this read's timestamp
    if (sitMTR == dto::K23SI_MTR_ZERO) {
        _readCache->insertInterval(request.key, request.key, request.mtr.timestamp);
    }

    // find the version deque for the key
    auto fiter = _indexer.find(request.key);
    if (fiter == _indexer.end()) {
        return _makeReadOK(nullptr);
    }
    auto& versions = fiter->second;
    auto viter = versions.begin();
    // position the version iterator at the version we should be returning
    while(viter != versions.end() && request.mtr.timestamp.compareCertain(viter->txnId.mtr.timestamp) < 0) {
        ++viter;
    }
    if (viter == versions.end()) {
        // happy case: we either had no versions, or all versions were newer than the requested timestamp
        return _makeReadOK(nullptr);
    }

    // happy case: either committed, or txn is reading its own write
    if (viter->status == DataRecord::Committed || viter->txnId.mtr == request.mtr) {
        return _makeReadOK(&(*viter));
    }
    // record is still pending and isn't from same transaction.

    if (sitMTR == dto::K23SI_MTR_ZERO) {
        // this is a fresh read finding a WI. have to do a push
        sitMTR = viter->txnId.mtr;
        return _doPush(request.collectionName, viter->txnId, request.mtr, deadline)
            .then([this, sitMTR, request=std::move(request), deadline](auto&& winnerMTR) mutable {
                if (winnerMTR == sitMTR) {
                    // sitting transaction won. Abort the incoming request
                    return RPCResponse(dto::K23SIStatus::AbortConflict(), dto::K23SIReadResponse<Payload>{});
                }
                // incoming request won. re-run read logic
                return handleRead(std::move(request), sitMTR, deadline);
            });
    }
    // this is a read after a push and we still find a WI. This WI must be the exact same one we pushed against,
    // or else our read cache was not used correctly in code.
    K2ASSERT(sitMTR == viter->txnId.mtr, "bug in code: found WI after push");
    K2ASSERT(viter == versions.begin(), "must be at newest version if we found a write intent")

    // remove the WI from cache and queue it up for cleanup
    _queueWICleanup(std::move(*viter));
    versions.pop_front();
    return _makeReadOK(versions.begin() == versions.end() ? nullptr : &(versions[0]));
}

bool K23SIPartitionModule::_validateStaleWrite(dto::K23SIWriteRequest<Payload>& request, std::deque<DataRecord>& versions) {
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        return false;
    }
    // check read cache for R->W conflicts
    auto ts = _readCache->checkInterval(request.key, request.key);
    if (request.mtr.timestamp.compareCertain(ts) < 0) {
        // this key range was read more recently than this write
        K2DEBUG("Partition: " << _partition << ", read cache validation failed for key: " << request.key);
        return false;
    }

    // check if we have a committed value newer than the request. The latest committed
    // is either the first or second in deque as we may have at most one outstanding WI
    // NB(1) if we try to place a WI over a committed value from different transaction with same ts.end
    // (even if from different TSO), reject the incoming write in order to avoid weird read-my-write problem
    // for in-progress transactions
    // NB(2) we cannot allow writes past a committed value since a write has to imply a read causality, so
    // if a txn committed a value at time T5, then we must also assume they did a read at time T5
    // NB(3) if we encounter a WI, we check the second oldest version to see if there is a need to push.
    // If the second oldest is newer than we are, then we won't commit even if we win a PUSH against the WI.
    if (versions.size() > 0 && versions[0].status == DataRecord::Committed &&
        request.mtr.timestamp.compareCertain(versions[0].txnId.mtr.timestamp) <= 0) {
        // newest version is the latest committed and its newer than the request
        K2DEBUG("Partition: " << _partition << ", failing write older than latest commit for key " << request.key);
        return false;
    }
    else if (versions.size() > 1 && versions[0].status == DataRecord::WriteIntent &&
        request.mtr.timestamp.compareCertain(versions[1].txnId.mtr.timestamp) <= 0) {
        // second newest version is the latest committed and its newer than the request.
        // no need to push since this request would fail anyway against the committed value
        K2DEBUG("Partition: " << _partition << ", failing write older than latest commit for key " << request.key);
        return false;
    }

    K2DEBUG("Partition: " << _partition << ", stale write check passed for key " << request.key);
    return true;
}

seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::handleWrite(dto::K23SIWriteRequest<Payload>&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline) {
    // NB: failures in processing a write do not require that we set the TR state to aborted at the TRH. We rely on
    //     the client to do the correct thing and issue an abort on a failure.
    // NB: sitMTR will be ZERO for original requests or non-zero for post-PUSH, winning writes.

    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2DEBUG("Partition: " << _partition << ", failed validation for " << request.key);
        return RPCResponse(dto::K23SIStatus::RefreshCollection(), dto::K23SIWriteResponse{});
    }
    // at this point the request is valid. Check to see if we should be creating a TR
    // we want to create the TR now even if the write may fail due to some other constraints. In case
    // of such failure, the client is expected to come in and end the transaction with Abort
    if (request.designateTRH) {
        K2DEBUG("Partition: " << _partition << ", designating trh for key " << request.key);
        return _txnMgr.onAction(TxnRecord::Action::onCreate, {.trh=request.trh, .mtr=request.mtr})
        .then([this, request=std::move(request), sitMTR=std::move(sitMTR), deadline]() mutable {
            K2DEBUG("Partition: " << _partition << ", tr created and re-driving request for key " << request.key);
            request.designateTRH = false; // unset the flag and re-run
            return handleWrite(std::move(request), std::move(sitMTR), deadline);
        })
        .handle_exception_type([this](TxnManager::ClientError&) {
            // Failed to create
            K2DEBUG("Partition: " << _partition << ", failed creating TR");
            return RPCResponse(dto::K23SIStatus::AbortConflict(), dto::K23SIWriteResponse{});
        });
    }

    auto& versions = _indexer[request.key];
    if (!_validateStaleWrite(request, versions)) {
        K2DEBUG("Partition: " << _partition << ", request too old for key " << request.key);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld(), dto::K23SIWriteResponse{});
    }

    // check to see if we should push or if we're coming after a push and the WI is still here
    if (versions.size() > 0 && versions[0].status == DataRecord::WriteIntent) {
        auto& rec = versions[0];
        auto& rqmtr = request.mtr;

        if (sitMTR == rec.txnId.mtr) {
            K2DEBUG("Partition: " << _partition << ", post-push winner for key " << request.key);
            // this is a post-PUSH request which won over the siting WI and we still have the WI in cache
            _queueWICleanup(std::move(rec));
            versions.pop_front();
        }
        else if (rec.txnId.mtr != rqmtr) {
            // this is a write request finding a WI from a different transaction. Do another push with the remaining
            // deadline time.
            K2DEBUG("Partition: " << _partition << ", different WI found for key " << request.key);
            sitMTR = rec.txnId.mtr;
            return _doPush(request.collectionName, rec.txnId, request.mtr, deadline)
                .then([this, sitMTR, request = std::move(request), deadline](auto&& winnerMTR) mutable {
                    if (winnerMTR == sitMTR) {
                        // sitting transaction won. Abort the incoming request
                        K2DEBUG("Partition: " << _partition << ", push lost for key " << request.key);
                        return RPCResponse(dto::K23SIStatus::AbortConflict(), dto::K23SIWriteResponse{});
                    }
                    // incoming request won. re-run write logic
                    K2DEBUG("Partition: " << _partition << ", push won for key " << request.key);
                    return handleWrite(std::move(request), sitMTR, deadline);
                });
        }
    }

    // all checks passed - we're ready to place this WI as the latest version(at head of versions deque)
    return _createWI(std::move(request), versions, deadline).then([this]() mutable {
        K2DEBUG("Partition: " << _partition << ", WI created");
        return RPCResponse(dto::K23SIStatus::Created(), dto::K23SIWriteResponse{});
    });
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
K23SIPartitionModule::handleTxnPush(dto::K23SITxnPushRequest&& request) {
    K2DEBUG("Partition: " << _partition << ", push request for key " << request.key);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection(), dto::K23SITxnPushResponse());
    }
    TxnId txnId{.trh=std::move(request.key), .mtr=std::move(request.incumbentMTR)};
    TxnRecord& incumbent = _txnMgr.getTxnRecord(txnId);
    // the only state for which we'd directly abort is the Created state (we didn't have this txn)
    bool abortIncumbent = incumbent.state == TxnRecord::State::Created;
    if (incumbent.state == TxnRecord::State::InProgress) {
        // check the cases when we have to abort the incumbent
        // #1 abort based on priority
        if (incumbent.txnId.mtr.priority > request.challengerMTR.priority) { // bigger number means lower priority
            K2DEBUG("Partition: " << _partition << ", aborting incumbent for key " << request.key);
            abortIncumbent = true;
        }
        // #2 if equal, pick the newer transaction
        else if (incumbent.txnId.mtr.priority == request.challengerMTR.priority) {
            auto cmpResult = incumbent.txnId.mtr.timestamp.compareCertain(request.challengerMTR.timestamp);
            if (cmpResult == dto::Timestamp::LT) {
                K2DEBUG("Partition: " << _partition << ", aborting incumbent for key " << request.key);
                abortIncumbent = true;
            }
            // #3 if same priority and timestamp, abort on tso ID which must be unique
            if (incumbent.txnId.mtr.timestamp.tsoId() < request.challengerMTR.timestamp.tsoId()) {
                K2DEBUG("Partition: " << _partition << ", aborting incumbent for key " << request.key);
                abortIncumbent = true;
            }
            else {
                // make sure we don't have a bug - the timestamps cannot be the same
                K2ASSERT(incumbent.txnId.mtr.timestamp.tsoId() != request.challengerMTR.timestamp.tsoId(), "invalid timestamps detected");
            }
        }
    }
    if (abortIncumbent) {
        // challenger won
        return _txnMgr.onAction(TxnRecord::Action::onForceAbort, std::move(txnId)).then([mtr=std::move(request.challengerMTR)] {
            return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnPushResponse{.winnerMTR = std::move(mtr)});
        });
    }
    else {
        // incumbent won
        K2DEBUG("Partition: " << _partition << ", incumbent won for key " << request.key);
        return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnPushResponse{.winnerMTR = std::move(txnId.mtr)});
    }
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
K23SIPartitionModule::handleTxnEnd(dto::K23SITxnEndRequest&& request) {
    K2DEBUG("Partition: " << _partition << ", transaction end for txn=" << request.mtr << ", with " << request.action);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2DEBUG("Partition: " << _partition << ", transaction end too old for txn=" << request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection(), dto::K23SITxnEndResponse());
    }

    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        K2DEBUG("Partition: " << _partition << ", transaction end outside retention for txn=" << request.mtr);
        return _txnMgr.onAction(TxnRecord::Action::onRetentionWindowExpire,
                            {.trh=std::move(request.key), .mtr=std::move(request.mtr)})
                .then([]() {
                    return RPCResponse(dto::K23SIStatus::AbortRequestTooOld(), dto::K23SITxnEndResponse());
                });
    }
    TxnId txnId{.trh = std::move(request.key), .mtr = std::move(request.mtr)};

    // this action always needs to be executed against the transaction to see what would happen.
    // If we can successfully execute the action, then it's a success response. Otherwise, the user
    // receives an error response which is telling them that the transaction has been aborted
    auto action = request.action == dto::EndAction::Commit ? TxnRecord::Action::onEndCommit : TxnRecord::Action::onEndAbort;

    // store the write keys into the txnrecord
    TxnRecord& rec = _txnMgr.getTxnRecord(txnId);
    rec.writeKeys = std::move(request.writeKeys);

    // and just execute the transition
    return _txnMgr.onAction(action, std::move(txnId))
        .then([this] {
            // action was successful
            K2DEBUG("Partition: " << _partition << ", transaction ended");
            return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnEndResponse());
        })
        .handle_exception_type([this](TxnManager::ClientError&) {
            K2DEBUG("Partition: " << _partition << ", failed transaction end");
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed(), dto::K23SITxnEndResponse());
        });
}

seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
K23SIPartitionModule::handleTxnHeartbeat(dto::K23SITxnHeartbeatRequest&& request) {
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2DEBUG("Partition: " << _partition << ", txn hb too old txn=" << request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection(), dto::K23SITxnHeartbeatResponse());
    }
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        K2DEBUG("Partition: " << _partition << ", txn hb too old txn=" << request.mtr);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld(), dto::K23SITxnHeartbeatResponse());
    }

    return _txnMgr.onAction(TxnRecord::Action::onHeartbeat, TxnId{.trh=std::move(request.key), .mtr=std::move(request.mtr)})
    .then([this]() {
        // heartbeat was applied successfully
        K2DEBUG("Partition: " << _partition << ", txn hb success");
        return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnHeartbeatResponse());
    })
    .handle_exception_type([this] (TxnManager::ClientError&) {
        // there was a problem applying the heartbeat due to client's view of the TR state. Client should abort
        K2DEBUG("Partition: " << _partition << ", txn hb fail");
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed(), dto::K23SITxnHeartbeatResponse{});
    });
}

seastar::future<dto::K23SI_MTR>
K23SIPartitionModule::_doPush(String collectionName, TxnId sitTxnId, dto::K23SI_MTR pushMTR, FastDeadline deadline) {
    dto::K23SITxnPushRequest request{};
    request.collectionName = std::move(collectionName);
    request.incumbentMTR = std::move(sitTxnId.mtr);
    request.key = std::move(sitTxnId.trh);
    request.challengerMTR = std::move(pushMTR);
    return _cpo.PartitionRequest<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse, dto::Verbs::K23SI_TXN_PUSH>(deadline, request)
    .then([this](auto&& responsePair) {
        auto& [status, response] = responsePair;
        if (status != dto::K23SIStatus::OK()) {
            K2DEBUG("Partition: " << _partition << ", txn push failed");
            return seastar::make_exception_future<dto::K23SI_MTR>(TxnManager::ServerError());
        }
        return seastar::make_ready_future<dto::K23SI_MTR>(std::move(response.winnerMTR));
    });
}

void K23SIPartitionModule::_queueWICleanup(DataRecord&& rec) {
    DataRecord(std::move(rec)); // move the record here so that we can drop it
}

seastar::future<>
K23SIPartitionModule::_createWI(dto::K23SIWriteRequest<Payload>&& request, std::deque<DataRecord>& versions, FastDeadline deadline) {
    K2DEBUG("Partition: " << _partition << ", creating WI for key " << request.key);
    DataRecord rec;
    rec.key = std::move(request.key);
    // we need to copy this data into a new memory block so that we don't hold onto and fragment the transport memory
    rec.value.val = request.value.val.copy();
    rec.isTombstone = request.isDelete;
    rec.txnId = TxnId{.trh = std::move(request.trh), .mtr = std::move(request.mtr)};
    rec.status = DataRecord::WriteIntent;

    versions.push_front(std::move(rec));
    // TODO write to WAL
    return _persistence.makeCall(deadline);
}

seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
K23SIPartitionModule::handleTxnFinalize(dto::K23SITxnFinalizeRequest&& request) {
    // find the version deque for the key
    K2DEBUG("Partition: " << _partition << ", txn finalize for " << request.mtr);
    auto fiter = _indexer.find(request.key);
    if (fiter == _indexer.end() || fiter->second.empty()) {
        if (request.action == dto::EndAction::Abort) {
            // we don't have it but it was an abort anyway
            K2DEBUG("Partition: " << _partition << ", abort for missing key " << request.key << ", in txn " << request.mtr);
            return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnFinalizeResponse());
        }
        // we can't allow the commit since we don't have the write intent
        K2DEBUG("Partition: " << _partition << ", rejecting commit for missing key " << request.key << ", in txn " << request.mtr);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed(), dto::K23SITxnFinalizeResponse());
    }
    auto& versions = fiter->second;
    auto viter = versions.begin();
    // position the version iterator at the version we should be converting
    while (viter != versions.end() && request.mtr.timestamp.compareCertain(viter->txnId.mtr.timestamp) < 0) {
        ++viter;
    }

    TxnId txnId{.trh=std::move(request.trh), .mtr=std::move(request.mtr)};
    if (viter == versions.end() || viter->txnId != txnId) {
        // we don't have a record from this transaction
        if (request.action == dto::EndAction::Abort) {
            // we don't have it but it was an abort anyway
            K2DEBUG("Partition: " << _partition << ", abort for missing version " << request.key << ", in txn " << request.mtr);
            return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnFinalizeResponse());
        }
        // we can't allow the commit since we don't have the write intent and we don't have a committed version
        K2DEBUG("Partition: " << _partition << ", rejecting commit for missing version " << request.key << ", in txn " << request.mtr);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed(), dto::K23SITxnFinalizeResponse());
    }

    // we found a record from this transaction
    if (viter->status != DataRecord::WriteIntent) {
        // asked to commit and it was already committed
        if (request.action == dto::EndAction::Commit) {
            // we have it committed already
            K2DEBUG("Partition: " << _partition << ", committed already " << request.key << ", in txn " << request.mtr);
            return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnFinalizeResponse());
        }
        // we can't allow the abort since the record is already committed
        K2DEBUG("Partition: " << _partition << ", failing abort for committed already " << request.key << ", in txn " << request.mtr);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed(), dto::K23SITxnFinalizeResponse());
    }

    // it is a write intent
    if (request.action == dto::EndAction::Commit) {
        K2DEBUG("Partition: " << _partition << ", committing " << request.key << ", in txn " << request.mtr);
        viter->status = DataRecord::Committed;
    }
    else {
        K2DEBUG("Partition: " << _partition << ", aborting " << request.key << ", in txn " << request.mtr);
        // erase from version list
        versions.erase(viter);
        if (versions.empty()) {
            // if there are no versions left, erase the key from indexer
            _indexer.erase(fiter);
        }
    }
    return RPCResponse(dto::K23SIStatus::OK(), dto::K23SITxnFinalizeResponse());
}
} // ns k2
