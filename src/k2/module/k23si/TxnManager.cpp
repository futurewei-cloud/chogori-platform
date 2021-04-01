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

#include "TxnManager.h"

namespace k2 {

template <typename Func>
void TxnManager::_addBgTask(TxnRecord& rec, Func&& func) {
    K2LOG_D(log::skvsvr, "Adding background task");
    if (_stopping) {
        K2LOG_W(log::skvsvr, "Attempting to add a background task during shutdown");
        return;
    }

    rec.bgTaskFut = rec.bgTaskFut.then(std::forward<Func>(func));
}

TxnManager::TxnManager():
    _cpo(_config.cpoEndpoint()) {
}

void TxnRecord::unlinkHB(HBList& hblist) {
    if (hbLink.is_linked()) {
        hblist.erase(hblist.iterator_to(*this));
    }
}
void TxnRecord::unlinkRW(RWList& rwlist) {
    if (rwLink.is_linked()) {
        rwlist.erase(rwlist.iterator_to(*this));
    }
}

TxnManager::~TxnManager() {
    K2LOG_I(log::skvsvr, "dtor for cname={}", _collectionName);
    _hblist.clear();
    _rwlist.clear();
    for (auto& [key, trec]: _transactions) {
        K2LOG_W(log::skvsvr, "Shutdown dropping transaction: {}", trec);
    }
}

seastar::future<> TxnManager::start(const String& collectionName, dto::Timestamp rts, Duration hbDeadline, std::shared_ptr<Persistence> persistence) {
    K2LOG_D(log::skvsvr, "start");
    _collectionName = collectionName;
    _hbDeadline = hbDeadline;
    _persistence= persistence;
    updateRetentionTimestamp(rts);
    // We need to call this now so that a recent time is used for a new
    // transaction's heartbeat expiry if it comes in before the first heartbeat timer callback
    CachedSteadyClock::now(true);

    _hbTimer.setCallback([this] {
        K2LOG_D(log::skvsvr, "txn manager check hb");
        // refresh the clock
        auto now = CachedSteadyClock::now(true);
        return seastar::do_until(
            [this, now] {
                auto noHB = _hblist.empty() || _hblist.front().hbExpiry > now;
                auto noRW = _rwlist.empty() || _rwlist.front().rwExpiry.compareCertain(_retentionTs) > 0;
                return noHB && noRW;
            },
            [this, now] {
                if (!_hblist.empty() && _hblist.front().hbExpiry <= now) {
                    auto& tr = _hblist.front();
                    K2LOG_W(log::skvsvr, "heartbeat expired on: {}", tr);
                    _hblist.pop_front();
                    return _onAction(TxnRecord::Action::onHeartbeatExpire, tr)
                        .then([](auto&& status) {
                            if (!status.is2xxOK()) {
                                K2LOG_E(log::skvsvr, "Failed processing heartbeat: {}", status);
                            }

                            // NB, it is possible that we modified the txn state here. It is not necessary
                            // to ensure we persist this as it is a purely internal state change
                            return seastar::make_ready_future();
                        });
                }
                else if (!_rwlist.empty() && _rwlist.front().rwExpiry.compareCertain(_retentionTs) <= 0) {
                    auto& tr = _rwlist.front();
                    K2LOG_W(log::skvsvr, "rw expired on: {}", tr);
                    _rwlist.pop_front();
                    return _onAction(TxnRecord::Action::onRetentionWindowExpire, tr)
                        .then([](auto&& status) {
                            if (!status.is2xxOK()) {
                                K2LOG_E(log::skvsvr, "Failed processing RWE: {}", status);
                            }

                            // NB, it is possible that we modified the tn state here. it is not necessary
                            // to ensure we persist this as it is a purely internal state change.
                            // If we fail and we recover this txn, we would process an RWE on it upon recovery.
                            return seastar::make_ready_future();
                        });
                }
                K2LOG_E(log::skvsvr, "Heartbeat processing failure - expected to find either hb or rw expired item but none found");
                return seastar::make_ready_future();
            })
            .handle_exception([] (auto exc){
                K2LOG_W_EXC(log::skvsvr, exc, "caught exception while checking hb/rw expiration");
                return seastar::make_ready_future();
            });
    });
    _hbTimer.armPeriodic(_hbDeadline);

    return seastar::make_ready_future();
}

seastar::future<> TxnManager::gracefulStop() {
    K2LOG_I(log::skvsvr, "stopping txn mgr for coll={}", _collectionName);
    _stopping = true;
    return _hbTimer.stop()
        .then([this] {
            K2LOG_I(log::skvsvr, "hb stopped. stopping with {} active transactions", _transactions.size());
            std::vector<seastar::future<>> bgFuts;
            for (auto& [_, txn]: _transactions) {
                bgFuts.push_back(std::move(txn.bgTaskFut));
            }
            return seastar::when_all_succeed(bgFuts.begin(), bgFuts.end()).discard_result();
        })
        .then_wrapped([] (auto&& fut) {
            if (fut.failed()) {
                K2LOG_W_EXC(log::skvsvr, fut.get_exception(), "txn failed background task");
            }
            K2LOG_I(log::skvsvr, "stopped");
            return seastar::make_ready_future();
        });
}

void TxnManager::updateRetentionTimestamp(dto::Timestamp rts) {
    K2LOG_D(log::skvsvr, "retention ts now={}", rts)
    _retentionTs = rts;
}

seastar::future<std::tuple<Status, dto::K23SIInspectAllTxnsResponse>>
TxnManager::inspectTxns() {
    std::vector<dto::K23SIInspectTxnResponse> txns;
    txns.reserve(_transactions.size());

    for (auto &[_,txn]: _transactions) {
        dto::K23SIInspectTxnResponse resp{
            txn.txnId,
            txn.writeKeys,
            txn.rwExpiry,
            txn.syncFinalize,
            txn.state};

        txns.push_back(std::move(resp));
    }

    dto::K23SIInspectAllTxnsResponse response{std::move(txns)};
    return RPCResponse(dto::K23SIStatus::OK("Inspect all txns success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>> TxnManager::inspectTxn(dto::TxnId&& txnId) {
    auto it = _transactions.find(txnId);
    if (it == _transactions.end()) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound("TRH not found"), dto::K23SIInspectTxnResponse{});
    }

    dto::K23SIInspectTxnResponse response{
        it->second.txnId,
        it->second.writeKeys,
        it->second.rwExpiry,
        it->second.syncFinalize,
        it->second.state};
    return RPCResponse(dto::K23SIStatus::OK("Inspect txn success"), std::move(response));
}

TxnRecord& TxnManager::getTxnRecord(dto::TxnId&& txnId) {
    // we don't persist the record on create. If we have a sudden failure, we'd
    // just abort the transaction when it comes to commit.
    auto it = _transactions.insert({std::move(txnId), TxnRecord{}});
    if (it.second) {
        TxnRecord& rec = it.first->second;
        rec.txnId = it.first->first;
        rec.state = dto::TxnRecordState::Created;
        rec.rwExpiry = txnId.mtr.timestamp;
        rec.hbExpiry = CachedSteadyClock::now() + 2*_hbDeadline;

        _hblist.push_back(rec);
        _rwlist.push_back(rec);
        K2LOG_D(log::skvsvr, "created new txn record: {}", it.first->second);
    }
    else {
        K2LOG_D(log::skvsvr, "found existing txn record: {}", it.first->second);
    }
    return it.first->second;
}

seastar::future<Status> TxnManager::createTxn(dto::TxnId&& txnId) {
    return _onAction(TxnRecord::Action::onCreate, getTxnRecord(std::move(txnId)));
}

seastar::future<Status> TxnManager::heartbeat(dto::TxnId&& txnId) {
    return _onAction(TxnRecord::Action::onHeartbeat, getTxnRecord(std::move(txnId)));
}

bool _shouldAbortIncumbentOnPush(TxnRecord& incumbent, dto::K23SI_MTR& challengerMTR) {
    // Calculate if the incumbent would be potentially aborted based on conflict resolution
    bool abortIncumbent = false;
    // #1 abort based on priority
    if (incumbent.txnId.mtr.priority > challengerMTR.priority) {  // bigger number means lower priority
        K2LOG_D(log::skvsvr, "incumbent {} would lose push", incumbent.txnId);
        abortIncumbent = true;
    }
    // #2 if equal, pick the newer transaction
    else if (incumbent.txnId.mtr.priority == challengerMTR.priority) {
        auto cmpResult = incumbent.txnId.mtr.timestamp.compareCertain(challengerMTR.timestamp);
        if (cmpResult == dto::Timestamp::LT) {
            K2LOG_D(log::skvsvr, "incumbent {} would lose push", incumbent.txnId);
            abortIncumbent = true;
        } else if (cmpResult == dto::Timestamp::EQ) {
            // #3 if same priority and timestamp, abort on tso ID which must be unique
            if (incumbent.txnId.mtr.timestamp.tsoId() < challengerMTR.timestamp.tsoId()) {
                K2LOG_D(log::skvsvr, "incumbent {} would lose push", incumbent.txnId);
                abortIncumbent = true;
            } else {
                // make sure we don't have a bug - the timestamps cannot be the same
                K2ASSERT(log::skvsvr, incumbent.txnId.mtr.timestamp.tsoId() != challengerMTR.timestamp.tsoId(), "invalid timestamps detected");
            }
        }
    }
    // #3 abort the challenger
    else {
        // this branch isn't needed as it is the fall-through option, but keeping it here for clarity
        K2LOG_D(log::skvsvr, "challenger {} would lose push", challengerMTR);
        abortIncumbent = false;
    }

    return abortIncumbent;
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
TxnManager::push(dto::TxnId&& incumbentId, dto::K23SI_MTR&& challengerMTR) {
    TxnRecord& incumbent = getTxnRecord(std::move(incumbentId));
    bool abortIncumbent = _shouldAbortIncumbentOnPush(incumbent, challengerMTR);

    switch (incumbent.state) {
        case dto::TxnRecordState::Created:
            // incumbent did not exist. Perform a force-abort.
            return _onAction(TxnRecord::Action::onForceAbort, incumbent)
                .then([this] (auto&& status) {
                    if (!status.is2xxOK()) {
                        K2LOG_W(log::skvsvr, "Unable to process force abort for non-existent txn due to {}", status);
                        return RPCResponse(std::move(status), dto::K23SITxnPushResponse{});
                    }
                    K2LOG_D(log::skvsvr, "txn push challenger won against non-existent txn");
                    // we can only respond with success to challenger after a successful flush to persistence
                    // otherwise, it is possible for this partition to recover with incumbent in a good state
                    return _persistence->flush().then([] (auto&& flushStatus) {
                        if (!flushStatus.is2xxOK()) {
                            return RPCResponse(std::move(flushStatus), dto::K23SITxnPushResponse{});
                        }
                        return RPCResponse(dto::K23SIStatus::OK("challenger won in push"),
                                dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Abort,
                                                          .allowChallengerRetry = true});
                    });
                });
        case dto::TxnRecordState::InProgressPIP: {
            if (abortIncumbent) {
                // redrive the push after persistence has flushed
                return _persistence->flush()
                    .then([this, txnId=incumbent.txnId, challengerMTR=std::move(challengerMTR)] (auto&& fstatus) mutable {
                        if (!fstatus.is2xxOK()) {
                            return RPCResponse(std::move(fstatus), dto::K23SITxnPushResponse{});
                        }
                        // redrive the push
                        return push(std::move(txnId), std::move(challengerMTR));
                    });
            }
            // challenger shouldn't touch our WI
            return RPCResponse(dto::K23SIStatus::OK("incumbent won push"),
                               dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::None,
                                                         .allowChallengerRetry = false});
        }
        case dto::TxnRecordState::InProgress: {
            if (abortIncumbent) {
                return _onAction(TxnRecord::Action::onForceAbort, incumbent)
                    .then([this] (auto&& status) {
                        if (!status.is2xxOK()) {
                            K2LOG_W(log::skvsvr, "Unable to process force abort for in-progress txn due to {}", status);
                            return RPCResponse(std::move(status), dto::K23SITxnPushResponse{});
                        }
                        return _persistence->flush().then([] (auto&& flushStatus) {
                            if (!flushStatus.is2xxOK()) {
                                return RPCResponse(std::move(flushStatus), dto::K23SITxnPushResponse{});
                            }
                            return RPCResponse(dto::K23SIStatus::OK("challenger won in push"),
                                            dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Abort,
                                                                      .allowChallengerRetry = true});
                            });
                    });
            } else {
                return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                                    dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::None,
                                                             .allowChallengerRetry = false});
            }
            break;
        }
        case dto::TxnRecordState::AbortedPIP:
            // the Abort PIP states can respond with challenger win, provided we haven't tried to commit
            // if we've attempted a commit, don't finalize - we can just tell them to retry
            if (incumbent.hasAttemptedCommit) {
                // this is an extraordinary case of persistence failure causing us to switch from commit to abort
                // let's just have the challenger retry over the network
                return RPCResponse(dto::K23SIStatus::OK("no winner for push: incumbent may have committed"),
                                   dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::None,
                                                             .allowChallengerRetry = true});
            }
            // fall-through
        case dto::TxnRecordState::ForceAborted:
        case dto::TxnRecordState::Aborted:
            // let client know that incumbent has been aborted and they can retry
            return RPCResponse(dto::K23SIStatus::OK("challenger won in push since incumbent was already aborted"),
                                dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Abort,
                                                          .allowChallengerRetry = true}
            );
        case dto::TxnRecordState::ForceAbortedPIP:
            // chances are the challenger would win, but we can't finalize the WI just yet
            // e.g. in the situation InProgress--(onFA due to push) -->ForceAbortedPIP
            // if we fail and recover, the incumbent will be IPR and could successfully commit.
            // redrive the push after persistence has flushed
            return _persistence->flush()
                .then([this, txnId=incumbent.txnId, challengerMTR=std::move(challengerMTR)](auto&& fstatus) mutable {
                    if (!fstatus.is2xxOK()) {
                        return RPCResponse(std::move(fstatus), dto::K23SITxnPushResponse{});
                    }
                    // redrive the push
                    return push(std::move(txnId), std::move(challengerMTR));
                });
        case dto::TxnRecordState::CommittedPIP:
            // we expect commit to succeed so just tell the challenger to abort
            return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                                dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::None,
                                                          .allowChallengerRetry = false});
        case dto::TxnRecordState::Committed:
            return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                                dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Commit,
                                                          .allowChallengerRetry = false});
        case dto::TxnRecordState::FinalizedPIP:
            // possible race condition - the incumbent has just finished finalizing and
            // is being removed from memory. The caller should not see this as a WI anymore
            return RPCResponse(dto::K23SIStatus::OK("incumbent finalized in push"),
                                dto::K23SITxnPushResponse{.incumbentFinalization = incumbent.finalizeAction,
                                                          .allowChallengerRetry = incumbent.finalizeAction == dto::EndAction::Abort});
        default:
            K2ASSERT(log::skvsvr, false, "Invalid transaction state: {}", incumbent.state);
    }
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
TxnManager::endTxn(dto::K23SITxnEndRequest&& request) {
    if (request.mtr.timestamp.compareCertain(_retentionTs) < 0) {
        // At this point this txn has gone outside RWE. All participants will self-finalize their WIs to Aborts
        // The TR itself will be moved to FA and deleted by the TxnManager's RWE tracking
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request is outside retention window"), dto::K23SITxnEndResponse{});
    }

    // this action always needs to be executed against the transaction to see what would happen.
    // If we can successfully execute the action, then it's a success response. Otherwise, the user
    // receives an error response which is telling them that the transaction has been aborted
    auto action = request.action == dto::EndAction::Commit ? TxnRecord::Action::onCommit : TxnRecord::Action::onAbort;
    if (request.action == dto::EndAction::None) {
        K2LOG_D(log::skvsvr, "cannot end transaction with None action in request {}", request);
        return RPCResponse(dto::K23SIStatus::BadParameter("cannot end transaction with `None` end action"), dto::K23SITxnEndResponse{});
    }
    // store the write keys into the txnrecord
    TxnRecord& rec = getTxnRecord(dto::TxnId{.trh = std::move(request.key), .mtr = std::move(request.mtr)});
    if (rec.finalizeAction != dto::EndAction::None) {
        // the record indicates we've received an end request already (there is a finalize action)
        // this is only possible if there is a retry or some client bug
        K2LOG_D(log::skvsvr, "TxnEnd retry - transaction already has a finalize action {}", rec)
        return _endTxnRetry(rec, std::move(request));
    }
    rec.writeKeys = std::move(request.writeKeys);
    rec.syncFinalize = request.syncFinalize;
    rec.timeToFinalize = request.timeToFinalize;
    rec.finalizeAction = request.action;
    rec.hasAttemptedCommit = request.action == dto::EndAction::Commit;

    // and just execute the transition
    return _onAction(action, rec)
        .then([this] (auto&& status) {
            if (!status.is2xxOK()) {
                return RPCResponse(std::move(status), dto::K23SITxnEndResponse{});
            }

            return RPCResponse(dto::K23SIStatus::OK("transaction ended"), dto::K23SITxnEndResponse{});
        });
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
TxnManager::_endTxnRetry(TxnRecord& rec, dto::K23SITxnEndRequest&& request) {
    K2LOG_D(log::skvsvr, "duplicate end request {}, have= {}", request, rec);
    if (rec.finalizeAction != request.action ||
        rec.writeKeys != request.writeKeys) { // TODO add keyHash computed by client and compare here to verify contents
        K2LOG_D(log::skvsvr, "invalid txn end retry request: rec={}, request={}", rec, request);
        return RPCResponse(dto::K23SIStatus::BadParameter("end request retry does not match previous end request"),dto::K23SITxnEndResponse{});
    }

    // Upon a retry, respond after the current bgtask queue has drained (potential in-flight retries)
    seastar::promise<std::tuple<Status, dto::K23SITxnEndResponse>> prom;
    auto fut = prom.get_future();
    _addBgTask(rec,
        [prom=std::move(prom), this, txnId=rec.txnId, request=std::move(request)] () mutable {
            // after previous bg tasks completed, see if we've finalized
            auto it = _transactions.find(txnId);
            if (it == _transactions.end()) {
                // normally, we won't find a record in memory after the bg tasks have been done
                prom.set_value(std::tuple(dto::K23SIStatus::OK("transaction retry ended"), dto::K23SITxnEndResponse{}));
            }
            else {
                // tell the client to retry
                prom.set_value(std::tuple(dto::K23SIStatus::ServiceUnavailable("unable to end transaction"), dto::K23SITxnEndResponse{}));
            }
            return seastar::make_ready_future();
        });
    return fut;
}

seastar::future<Status> TxnManager::_onAction(TxnRecord::Action action, TxnRecord& rec) {
    auto state = rec.state;
    K2LOG_D(log::skvsvr, "Processing action {}, for state {}, in txn {}", action, state, rec);
    switch (state) {
        case dto::TxnRecordState::Created:
            // We did not have a transaction record and it was just created
            switch (action) {
                case TxnRecord::Action::onCreate: // happy case
                    return _inProgressPIP(rec);
                case TxnRecord::Action::onForceAbort:
                    return _forceAbortedPIP(rec);
                case TxnRecord::Action::onHeartbeat: // illegal - create a ForceAborted entry and wait for End
                    return _forceAbortedPIP(rec)
                        .then([] (auto&&) {
                            // respond with failure since we had to force abort but were asked to heartbeat
                            return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("cannot heartbeat transaction since it doesn't exist"));
                        });
                case TxnRecord::Action::onCommit:  // create an entry in Aborted state so that it can be finalized
                    rec.finalizeAction = dto::EndAction::Abort;
                    return _endPIP(rec)
                        .then([] (auto&&) {
                            // respond with failure since we had to abort but were asked to commit
                            return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("cannot commit transaction since it has been aborted"));
                        });
                case TxnRecord::Action::onAbort:  // create an entry in Aborted state so that it can be finalized
                    return _endPIP(rec);
                default: // anything else we just count as internal error
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.txnId, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("invalid transition"));
            };
        case dto::TxnRecordState::InProgressPIP:
            switch (action) {
                case TxnRecord::Action::onPersistFail:
                    return _forceAbortedPIP(rec).then([](auto&&) {
                        return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("Unable to start transaction due to persistence failure. Aborting..."));
                    });
                case TxnRecord::Action::onPersistSucceed: // happy case
                    return _inProgress(rec);
                case TxnRecord::Action::onCreate:
                case TxnRecord::Action::onAbort:
                case TxnRecord::Action::onCommit:
                case TxnRecord::Action::onRetentionWindowExpire:
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onHeartbeat:
                    // we want the client to retry these. Return a retryable error
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::ServiceUnavailable("retry: persistence in progress"));
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.txnId, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("invalid transition"));
            }
        case dto::TxnRecordState::InProgress:
            switch (action) {
                case TxnRecord::Action::onCreate: // no-op - stay in same state
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onHeartbeat: {
                    K2LOG_D(log::skvsvr, "Processing heartbeat for {}", rec);

                    rec.unlinkHB(_hblist);
                    rec.hbExpiry = CachedSteadyClock::now() + 2 * _hbDeadline;
                    _hblist.push_back(rec);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                }
                case TxnRecord::Action::onCommit: // happy case
                case TxnRecord::Action::onAbort:
                    return _endPIP(rec);
                case TxnRecord::Action::onForceAbort:             // asked to force-abort (e.g. on PUSH)
                case TxnRecord::Action::onRetentionWindowExpire:  // we've had this transaction for too long
                case TxnRecord::Action::onHeartbeatExpire:        // originator didn't hearbeat on time
                    return _forceAbortedPIP(rec);
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}, in state: {}", rec.txnId, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("invalid transition"));
            };
        case dto::TxnRecordState::ForceAbortedPIP:
            switch (action) {
                case TxnRecord::Action::onPersistFail:
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("Unable to force-abort transaction due to persistence failure"));
                case TxnRecord::Action::onPersistSucceed: // happy case
                    return _forceAborted(rec);
                case TxnRecord::Action::onCreate:
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onAbort:
                case TxnRecord::Action::onCommit:
                case TxnRecord::Action::onHeartbeat:
                    // we want the client to retry these. Return a retryable error
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::ServiceUnavailable("retry: persistence in progress"));
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.txnId, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("invalid transition"));
            }
        case dto::TxnRecordState::ForceAborted:
            switch (action) {
                case TxnRecord::Action::onCreate: // this has been aborted already. Signal the client to issue endAbort
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("cannot create transaction since it has been force-aborted"));
                case TxnRecord::Action::onForceAbort:  // no-op
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onRetentionWindowExpire:
                    return _finalizedPIP(rec);
                case TxnRecord::Action::onCommit:
                    return _endPIP(rec)
                        .then([] (auto&&) {
                            // we respond with failure here anyway since we had to abort but were asked to commit
                            return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("cannot commit transaction since it has been force-aborted"));
                        });
                case TxnRecord::Action::onAbort:
                    return _endPIP(rec);
                case TxnRecord::Action::onHeartbeat: // signal client to abort
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("cannot heartbeat transaction since it has been force-aborted"));
                case TxnRecord::Action::onFinalizeComplete:
                case TxnRecord::Action::onHeartbeatExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.txnId);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid transition"));
            };
        case dto::TxnRecordState::AbortedPIP:
            switch (action) {
                case TxnRecord::Action::onPersistFail:
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("Unable to abort transaction due to persistence failure"));
                case TxnRecord::Action::onPersistSucceed:
                    return _end(rec);
                case TxnRecord::Action::onForceAbort:  // no-op
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onAbort:
                case TxnRecord::Action::onCommit:
                case TxnRecord::Action::onHeartbeat:
                    // we want the client to retry these. Return a retryable error
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::ServiceUnavailable("retry: persistence in progress"));
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.txnId, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("invalid transition"));
            }
        case dto::TxnRecordState::Aborted:
            switch (action) {
                case TxnRecord::Action::onCreate: // signal client to abort
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onCommit:
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("cannot commit transaction since it has been aborted"));
                case TxnRecord::Action::onAbort: // accept this to be re-entrant
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onFinalizeComplete: // on to deleting this record
                    return _finalizedPIP(rec);
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.txnId);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid transition"));
            };
        case dto::TxnRecordState::CommittedPIP:
            switch (action) {
                case TxnRecord::Action::onPersistFail: {
                    rec.finalizeAction = dto::EndAction::Abort;
                    return _endPIP(rec).then([] (auto&&){
                        return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("Unable to commit transaction due to persistence failure. Aborting"));
                    });
                }
                case TxnRecord::Action::onPersistSucceed:
                    return _end(rec);
                case TxnRecord::Action::onAbort:
                case TxnRecord::Action::onCommit:
                case TxnRecord::Action::onHeartbeat:
                    // we want the client to retry these. Return a retryable error
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::ServiceUnavailable("retry: persistence in progress"));
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.txnId, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("invalid transition"));
            }
        case dto::TxnRecordState::Committed:
            switch (action) {
                case TxnRecord::Action::onCreate: // signal client to abort
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onAbort:
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("cannot abort transaction since it has been committed"));
                case TxnRecord::Action::onCommit: // accept this to be re-entrant
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onFinalizeComplete:
                    return _finalizedPIP(rec);
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.txnId);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid transition"));
            };
        case dto::TxnRecordState::FinalizedPIP:
            switch (action) {
                case TxnRecord::Action::onAbort:
                case TxnRecord::Action::onCreate:
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                case TxnRecord::Action::onCommit: // accept this to be re-entrant
                case TxnRecord::Action::onFinalizeComplete:
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.txnId);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid transition"));
            };
        default:
            K2LOG_E(log::skvsvr, "Invalid record state ({}), for action: {}, in txnid: {}", state, action, rec.txnId);
            return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid record state"));
    }
}

seastar::future<Status> TxnManager::_inProgressPIP(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to InProgressPIP for {}", rec);
    rec.state = dto::TxnRecordState::InProgressPIP;
    _addBgTask(rec,
        [this, &rec] {
            return _persistence->append_cont(rec)
            .then([this, &rec] (auto&& status) {
                K2LOG_D(log::skvsvr, "persist completed for InProgressPIP of {} with {}", rec, status);
                if (!status.is2xxOK()) {
                    // flush didn't succeed
                    K2LOG_E(log::skvsvr, "persist failed for InProgressPIP of {} with {}", rec, status);
                    return _onAction(TxnRecord::Action::onPersistFail, rec);
                }
                return _onAction(TxnRecord::Action::onPersistSucceed, rec);
            }).discard_result();
        });
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_inProgress(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to inProgress for {}", rec);
    rec.state = dto::TxnRecordState::InProgress;
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_forceAbortedPIP(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to ForceAbortedPIP for {}", rec);
    rec.state = dto::TxnRecordState::ForceAbortedPIP;
    _addBgTask(rec,
        [this, &rec] {
            return _persistence->append_cont(rec)
            .then([this, &rec] (auto&& status) {
                K2LOG_D(log::skvsvr, "persist completed for ForceAbortedPIP of {} with {}", rec, status);
                if (!status.is2xxOK()) {
                    // flush didn't succeed
                    K2LOG_E(log::skvsvr, "persist failed for ForceAbortedPIP of {} with {}", rec, status);
                    return _onAction(TxnRecord::Action::onPersistFail, rec);
                }
                return _onAction(TxnRecord::Action::onPersistSucceed, rec);
            }).discard_result();
        });
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_forceAborted(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to forceAborted for {}", rec);
    // set state
    rec.state = dto::TxnRecordState::ForceAborted;
    // there is no longer a heartbeat expectation
    rec.unlinkHB(_hblist);
    // we still want to keep the record inked in the retention window since we want to take action if it goes past the RWE
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_endPIP(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status EndPIP for {}", rec);
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);

    rec.state = rec.finalizeAction == dto::EndAction::Commit ? dto::TxnRecordState::CommittedPIP : dto::TxnRecordState::AbortedPIP;
    _addBgTask(rec,
        [this, &rec] {
            return _persistence->append_cont(rec)
            .then([this, &rec] (auto&& status) {
                K2LOG_D(log::skvsvr, "persist completed for EndPIP of {} with {}", rec, status);
                if (!status.is2xxOK()) {
                    // flush didn't succeed
                    K2LOG_E(log::skvsvr, "persist failed for EndPIP of {} with {}", rec, status);
                    return _onAction(TxnRecord::Action::onPersistFail, rec);
                }
                return _onAction(TxnRecord::Action::onPersistSucceed, rec);
            }).discard_result();
        });
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_end(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting state End for {}", rec);
    rec.state = rec.finalizeAction == dto::EndAction::Commit ? dto::TxnRecordState::Committed : dto::TxnRecordState::Aborted;

    auto timeout = (10s + _config.writeTimeout() * rec.writeKeys.size()) / _config.finalizeBatchSize();

    if (rec.syncFinalize) {
        return _finalizeTransaction(rec, FastDeadline(timeout));
    }

    // we're doing async finalize. enqueue in background tasks
    _addBgTask(rec,
        [this, &rec, timeout] {
            return seastar::sleep(rec.timeToFinalize)
                .then([this, &rec, timeout] {
                    return _finalizeTransaction(rec, FastDeadline(timeout));
                }).
                then([] (auto&& status) {
                    if (!status.is2xxOK()) {
                        K2LOG_E(log::skvsvr, "Failed to finalize transaction due to: {}", status);
                    }
                    return seastar::make_ready_future();
                }).discard_result();
        });

    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_finalizedPIP(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to FinalizedPIP for {}", rec);
    // set state
    rec.state = dto::TxnRecordState::FinalizedPIP;
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);

    _addBgTask(rec,
        [this, &rec] {
            return _persistence->append_cont(rec)
                .then([this, &rec](auto&& status) {
                    K2LOG_D(log::skvsvr, "persist completed for FinalizedPIP of {} with {}", rec, status);
                    if (!status.is2xxOK()) {
                        // flush didn't succeed
                        K2LOG_E(log::skvsvr, "persist failed for FinalizedPIP of {} with {}", rec, status);
                        // This is not a crash condition as we already have the end
                        // action in the WAL. Txn state should be consistent
                    }

                    K2LOG_D(log::skvsvr, "Erasing txn record: {}", rec);
                    rec.unlinkRW(_rwlist);
                    rec.unlinkHB(_hblist);
                    _transactions.erase(rec.txnId);
                    return seastar::make_ready_future();
                });
        });
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_finalizeTransaction(TxnRecord& rec, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "Finalizing {}", rec);
    //TODO we need to keep trying to finalize in cases of failures.
    // this needs to be done in a rate-limited fashion. For now, we just try some configurable number of times and give up
    return seastar::do_with((uint64_t)0, [this, &rec, deadline] (auto& batchStart) {
        return seastar::do_until(
            [this, &rec, &batchStart] { return batchStart >= rec.writeKeys.size(); },
            [this, &rec, &batchStart, deadline] {
                auto start = rec.writeKeys.begin() + batchStart;
                batchStart += std::min(_config.finalizeBatchSize(), rec.writeKeys.size() - batchStart);
                auto end = rec.writeKeys.begin() + batchStart;
                return seastar::parallel_for_each(start, end, [&rec, this, deadline](dto::Key& key) {
                    dto::K23SITxnFinalizeRequest request{};
                    request.key = key;
                    request.collectionName = _collectionName;
                    request.mtr = rec.txnId.mtr;
                    request.trh = rec.txnId.trh;
                    if (rec.state == dto::TxnRecordState::Committed) {
                        request.action = dto::EndAction::Commit;
                    }
                    else if (rec.state == dto::TxnRecordState::Aborted) {
                        request.action = dto::EndAction::Abort;
                    }
                    else {
                        K2LOG_E(log::skvsvr, "invalid txn record state during finalization: {}", rec);
                        return seastar::make_exception_future<>(std::runtime_error("Invalid internal transaction state during finalization"));
                    }
                    K2LOG_D(log::skvsvr, "Finalizing req={}", request);
                    return seastar::do_with(std::move(request), [&rec, this, deadline](auto& request) {
                        return _cpo.PartitionRequest<dto::K23SITxnFinalizeRequest,
                                                    dto::K23SITxnFinalizeResponse,
                                                    dto::Verbs::K23SI_TXN_FINALIZE>
                        (deadline, request, _config.finalizeRetries())
                        .then([&request](auto&& responsePair) {
                            auto& [status, response] = responsePair;
                            if (!status.is2xxOK()) {
                                K2LOG_E(log::skvsvr, "Finalize request did not succeed for {}, status={}", request, status);
                                return seastar::make_exception_future<>(std::runtime_error(fmt::format("finalize request failed after retrying due to {}", status)));
                            }
                            K2LOG_D(log::skvsvr, "Finalize request succeeded for {}", request);
                            return seastar::make_ready_future<>();
                        }).finally([]{ K2LOG_D(log::skvsvr, "finalize call finished");});
                    });
                }).then([&batchStart, &rec]{
                    K2LOG_D(log::skvsvr, "Batch done, now at: {}, in {}", batchStart, rec);
                });
            }
        );
    })
    .then_wrapped([this, &rec] (auto&& fut) {
        if (fut.failed()) {
            auto exc = fut.get_exception();
            K2LOG_W_EXC(log::skvsvr, exc, "Unable to finalize {}. Leaving in memory", rec);
            return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("unable to finalize due to internal error"));
        }
        K2LOG_D(log::skvsvr, "finalize completed for: {}", rec);
        return _onAction(TxnRecord::Action::onFinalizeComplete, rec);
    });
}

}  // namespace k2
