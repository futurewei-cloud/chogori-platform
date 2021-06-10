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

#include <boost/range/irange.hpp>

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

TxnManager::TxnManager() {
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
    _cpo.init(_config.cpoEndpoint());
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
            .mtr=txn.mtr,
            .trh=txn.trh,
            .trhCollection=_collectionName,
            .writeRanges=txn.writeRanges,
            .rwExpiry=txn.rwExpiry,
            .syncFinalize=txn.syncFinalize,
            .state=txn.state,
            .finalizeAction=txn.finalizeAction};

        txns.push_back(std::move(resp));
    }

    dto::K23SIInspectAllTxnsResponse response{std::move(txns)};
    return RPCResponse(dto::K23SIStatus::OK("Inspect all txns success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>> TxnManager::inspectTxn(dto::Timestamp txnTimestamp) {
    auto it = _transactions.find(txnTimestamp);
    if (it == _transactions.end()) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound("TRH not found"), dto::K23SIInspectTxnResponse{});
    }

    dto::K23SIInspectTxnResponse response{
        .mtr=it->second.mtr,
        .trh=it->second.trh,
        .trhCollection=_collectionName,
        .writeRanges=it->second.writeRanges,
        .rwExpiry=it->second.rwExpiry,
        .syncFinalize=it->second.syncFinalize,
        .state=it->second.state,
        .finalizeAction=it->second.finalizeAction};
    return RPCResponse(dto::K23SIStatus::OK("Inspect txn success"), std::move(response));
}

TxnRecord& TxnManager::getTxnRecord(dto::K23SI_MTR&& mtr, dto::Key trhKey) {
    // we don't persist the record on create. If we have a sudden failure, we'd
    // just abort the transaction when it comes to commit.
    auto it = _transactions.insert({mtr.timestamp, TxnRecord{}});
    if (it.second) {
        TxnRecord& rec = it.first->second;
        rec.mtr=mtr;
        rec.trh=std::move(trhKey);
        rec.rwExpiry=mtr.timestamp;
        rec.hbExpiry=CachedSteadyClock::now() + 2*_hbDeadline;
        _hblist.push_back(rec);
        _rwlist.push_back(rec);
        rec.state=dto::TxnRecordState::Created;

        K2LOG_D(log::skvsvr, "created new txn record: {}", it.first->second);
    }
    else {
        K2LOG_D(log::skvsvr, "found existing txn record: {}", it.first->second);
    }
    return it.first->second;
}

seastar::future<Status> TxnManager::createTxn(dto::K23SI_MTR&& mtr, dto::Key trhKey) {
    return _onAction(TxnRecord::Action::onCreate, getTxnRecord(std::move(mtr), std::move(trhKey)));
}

seastar::future<Status> TxnManager::heartbeat(dto::K23SI_MTR&& mtr, dto::Key trhKey) {
    return _onAction(TxnRecord::Action::onHeartbeat, getTxnRecord(std::move(mtr), std::move(trhKey)));
}

// return true if the challenge was successful (challenger wins over incumbent)
bool _evaluateChallenge(TxnRecord& incumbent, dto::K23SI_MTR& challengerMTR) {
    // Calculate if the incumbent would lose the challenge based on conflict resolution
    bool incumbentLostConflict = false;
    // #1 abort based on priority
    if (incumbent.mtr.priority > challengerMTR.priority) {  // bigger number means lower priority
        K2LOG_D(log::skvsvr, "incumbent {} could lose push", incumbent.mtr);
        incumbentLostConflict = true;
    }
    // #2 if equal, pick the newer transaction
    else if (incumbent.mtr.priority == challengerMTR.priority) {
        // Note that compareCertain will order timestamps based on tsoID in cases where raw times are equivalent,
        // thus guaranteeing strict ordering for non-identical timestamps.
        auto cmpResult = incumbent.mtr.timestamp.compareCertain(challengerMTR.timestamp);
        if (cmpResult == dto::Timestamp::LT) {
            K2LOG_D(log::skvsvr, "incumbent {} could lose push", incumbent.mtr);
            incumbentLostConflict = true;
        } else if (cmpResult == dto::Timestamp::EQ) {
            K2ASSERT(log::skvsvr, incumbent.mtr.timestamp.tsoId() != challengerMTR.timestamp.tsoId(), "invalid timestamps detected");
        }
    }
    // #3 abort the challenger
    else {
        // this branch isn't needed as it is the fall-through option, but keeping it here for clarity
        K2LOG_D(log::skvsvr, "challenger {} could lose push", challengerMTR);
        incumbentLostConflict = false;
    }

    return incumbentLostConflict;
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
TxnManager::push(dto::K23SI_MTR&& incumbentMTR, dto::K23SI_MTR&& challengerMTR, dto::Key trhKey) {
    TxnRecord& incumbent = getTxnRecord(std::move(incumbentMTR), std::move(trhKey));

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
                    return RPCResponse(dto::K23SIStatus::OK("challenger won in push"),
                                       dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Abort,
                                                                 .allowChallengerRetry = true});
                });
        case dto::TxnRecordState::InProgress: {
            if (_evaluateChallenge(incumbent, challengerMTR)) {
                return _onAction(TxnRecord::Action::onForceAbort, incumbent)
                    .then([this] (auto&& status) {
                        if (!status.is2xxOK()) {
                            K2LOG_W(log::skvsvr, "Unable to process force abort for in-progress txn due to {}", status);
                            return RPCResponse(std::move(status), dto::K23SITxnPushResponse{});
                        }
                        return RPCResponse(dto::K23SIStatus::OK("challenger won in push"),
                                            dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Abort,
                                                                      .allowChallengerRetry = true});
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
        case dto::TxnRecordState::CommittedPIP:
            // we expect commit to succeed. Challenger should retry if they would've won over an in-progress incumbent
            return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                               dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::None,
                                                         .allowChallengerRetry = challengerMTR.timestamp.compareCertain(incumbent.mtr.timestamp) == dto::Timestamp::GT});
        case dto::TxnRecordState::Committed:
            // Challenger should retry if they are newer than the committed value
            return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                               dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Commit,
                                                         .allowChallengerRetry = true});
        case dto::TxnRecordState::FinalizedPIP:
            // possible race condition - the incumbent has just finished finalizing and
            // is being removed from memory. The caller should not see this as a WI anymore
            return RPCResponse(dto::K23SIStatus::OK("incumbent finalized in push"),
                               dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::None,
                                                         .allowChallengerRetry = true});
        default:
            K2ASSERT(log::skvsvr, false, "Invalid transaction state: {}", incumbent.state);
    }
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
TxnManager::endTxn(dto::K23SITxnEndRequest&& request) {
    // this action always needs to be executed against the transaction to see what would happen.
    // If we can successfully execute the action, then it's a success response. Otherwise, the user
    // receives an error response which is telling them that the transaction has been aborted
    auto action = request.action == dto::EndAction::Commit ? TxnRecord::Action::onCommit : TxnRecord::Action::onAbort;
    if (request.action == dto::EndAction::None) {
        K2LOG_D(log::skvsvr, "cannot end transaction with None action in request {}", request);
        return RPCResponse(dto::K23SIStatus::BadParameter("cannot end transaction with `None` end action"), dto::K23SITxnEndResponse{});
    }
    // store the write keys into the txnrecord
    TxnRecord& rec = getTxnRecord(std::move(request.mtr), std::move(request.key));
    if (rec.finalizeAction != dto::EndAction::None) {
        // the record indicates we've received an end request already (there is a finalize action)
        // this is only possible if there is a retry or some client bug
        K2LOG_D(log::skvsvr, "TxnEnd retry - transaction already has a finalize action {}", rec)
        return _endTxnRetry(rec, std::move(request));
    }

    if (rec.mtr.timestamp.compareCertain(_retentionTs) < 0) {
        // At this point this txn has gone outside RWE. All participants will self-finalize their WIs to Aborts
        // The TR itself will be moved to FA and deleted by the TxnManager's RWE tracking
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request is outside retention window"), dto::K23SITxnEndResponse{});
    }

    rec.writeRanges = std::move(request.writeRanges);
    rec.syncFinalize = request.syncFinalize;
    rec.timeToFinalize = request.timeToFinalize;
    rec.finalizeAction = request.action;
    if (request.action == dto::EndAction::Commit) {
        rec.hasAttemptedCommit = true;
    }

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
    if (rec.finalizeAction != request.action) {
        K2LOG_D(log::skvsvr, "invalid txn end retry request: rec={}, request={}", rec, request);
        return RPCResponse(dto::K23SIStatus::BadParameter("end request retry does not match previous end request"),dto::K23SITxnEndResponse{});
    }

    // TODO Track the number of such occurrences and decide if we should do something about it.
    // Current proposal is to at least keep an LRU of finalized txns so that we can respond to such retries.
    return RPCResponse(dto::K23SIStatus::KeyNotFound("unable to end transaction"), dto::K23SITxnEndResponse{});
}

seastar::future<Status> TxnManager::_onAction(TxnRecord::Action action, TxnRecord& rec) {
    auto state = rec.state;
    K2LOG_D(log::skvsvr, "Processing action {}, for state {}, in txn {}", action, state, rec);
    switch (state) {
        case dto::TxnRecordState::Created:
            // We did not have a transaction record and it was just created
            switch (action) {
                case TxnRecord::Action::onCreate: // happy case
                    return _inProgress(rec);
                case TxnRecord::Action::onForceAbort:
                    return _forceAborted(rec);
                case TxnRecord::Action::onHeartbeat: // illegal - create a ForceAborted entry and wait for End
                    K2LOG_W(log::skvsvr, "Heartbeat received before txn start in txn {}", rec);
                    return _forceAborted(rec)
                        .then([] (auto&&) {
                            // respond with failure since we had to force abort but were asked to heartbeat
                            return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("cannot heartbeat transaction since it doesn't exist"));
                        });
                case TxnRecord::Action::onCommit:  // create an entry in Aborted state so that it can be finalized
                    K2LOG_W(log::skvsvr, "Commit received before txn start in txn {}", rec);
                    return _abortPIP(rec)
                        .then([] (auto&&) {
                            // respond with failure since we had to abort but were asked to commit
                            return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("cannot commit transaction since it has been aborted"));
                        });
                case TxnRecord::Action::onAbort:  // create an entry in Aborted state so that it can be finalized
                    K2LOG_W(log::skvsvr, "Abort received before txn start in txn {}", rec);
                    return _abortPIP(rec);
                default: // anything else we just count as internal error
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.mtr, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("invalid transition"));
            };
        case dto::TxnRecordState::InProgress:
            switch (action) {
                case TxnRecord::Action::onCreate: // no-op - stay in same state
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::Created);
                case TxnRecord::Action::onHeartbeat: {
                    K2LOG_D(log::skvsvr, "Processing heartbeat for {}", rec);

                    rec.unlinkHB(_hblist);
                    rec.hbExpiry = CachedSteadyClock::now() + 2 * _hbDeadline;
                    _hblist.push_back(rec);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                }
                case TxnRecord::Action::onCommit: // happy case
                    return _commitPIP(rec);
                case TxnRecord::Action::onAbort:
                    return _abortPIP(rec);
                case TxnRecord::Action::onForceAbort:             // asked to force-abort (e.g. on PUSH)
                case TxnRecord::Action::onRetentionWindowExpire:  // we've had this transaction for too long
                case TxnRecord::Action::onHeartbeatExpire:        // originator didn't hearbeat on time
                    return _forceAborted(rec);
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}, in state: {}", rec.mtr, state);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("invalid transition"));
            };
        case dto::TxnRecordState::ForceAborted:
            switch (action) {
                case TxnRecord::Action::onCreate: // this has been aborted already. Signal the client to issue endAbort
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("cannot create transaction since it has been force-aborted"));
                case TxnRecord::Action::onForceAbort:  // no-op
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onRetentionWindowExpire:
                    // manage rw expiry
                    rec.unlinkRW(_rwlist);
                    _transactions.erase(rec.mtr.timestamp);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK("Force abort exceeded RWE"));
                case TxnRecord::Action::onCommit:
                    return _abortPIP(rec)
                        .then([] (auto&&) {
                            // we respond with failure here anyway since we had to abort but were asked to commit
                            return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("cannot commit transaction since it has been force-aborted"));
                        });
                case TxnRecord::Action::onAbort:
                    return _abortPIP(rec);
                case TxnRecord::Action::onHeartbeat: // signal client to abort
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("cannot heartbeat transaction since it has been force-aborted"));
                case TxnRecord::Action::onFinalizeComplete:
                case TxnRecord::Action::onHeartbeatExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.mtr);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid transition"));
            };
        case dto::TxnRecordState::AbortedPIP:
            switch (action) {
                case TxnRecord::Action::onPersistFail:
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("Unable to abort transaction due to persistence failure"));
                case TxnRecord::Action::onPersistSucceed:
                    return _abort(rec);
                case TxnRecord::Action::onForceAbort:  // no-op
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
                case TxnRecord::Action::onAbort:
                case TxnRecord::Action::onCommit:
                case TxnRecord::Action::onHeartbeat:
                    // we want the client to retry these. Return a retryable error
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::ServiceUnavailable("retry: persistence in progress"));
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.mtr, state);
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
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.mtr);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid transition"));
            };
        case dto::TxnRecordState::CommittedPIP:
            switch (action) {
                case TxnRecord::Action::onPersistFail: {
                    return _abortPIP(rec).then([] (auto&&) {
                        return seastar::make_ready_future<Status>(dto::K23SIStatus::InternalError("Unable to commit transaction due to persistence failure. Aborting"));
                    });
                }
                case TxnRecord::Action::onPersistSucceed:
                    return _commit(rec);
                case TxnRecord::Action::onAbort:
                case TxnRecord::Action::onCommit:
                case TxnRecord::Action::onHeartbeat:
                    // we want the client to retry these. Return a retryable error
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::ServiceUnavailable("retry: persistence in progress"));
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, rec.mtr, state);
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
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.mtr);
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
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", rec.mtr);
                    return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid transition"));
            };
        default:
            K2LOG_E(log::skvsvr, "Invalid record state ({}), for action: {}, in txnid: {}", state, action, rec.mtr);
            return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("invalid record state"));
    }
}

seastar::future<Status> TxnManager::_inProgress(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to inProgress for {}", rec);
    rec.state = dto::TxnRecordState::InProgress;
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

seastar::future<Status> TxnManager::_commitPIP(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to CommitPIP for {}", rec);
    rec.finalizeAction = dto::EndAction::Commit;
    rec.state = dto::TxnRecordState::CommittedPIP;
    return _endPIPHelper(rec);
}

seastar::future<Status> TxnManager::_abortPIP(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to AbortPIP for {}", rec);
    rec.finalizeAction = dto::EndAction::Abort;
    rec.state = dto::TxnRecordState::AbortedPIP;
    return _endPIPHelper(rec);
}

seastar::future<Status> TxnManager::_endPIPHelper(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "EndPIP for {}", rec);
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);

    auto finfut =  _persistence->append_cont(rec)
            .then([this, &rec] (auto&& status) {
                K2LOG_D(log::skvsvr, "persist completed for EndPIP of {} with {}", rec, status);
                if (!status.is2xxOK()) {
                    // flush didn't succeed
                    K2LOG_E(log::skvsvr, "persist failed for EndPIP of {} with {}", rec, status);
                    return _onAction(TxnRecord::Action::onPersistFail, rec);
                }
                return _onAction(TxnRecord::Action::onPersistSucceed, rec);
            });
    if (rec.syncFinalize) {
        // flush manually here in order to complete the append operation above. Only then can we
        // return the continuation of append
        return _persistence->flush().then([finfut=std::move(finfut)] (auto&& flushStatus) mutable {
            if (!flushStatus.is2xxOK()) {
                return seastar::make_ready_future<Status>(std::move(flushStatus));
            }
            return std::move(finfut);
        });
    }
    _addBgTask(rec, [finfut=std::move(finfut)] () mutable { return finfut.discard_result();});
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_commit(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to Commit for {}", rec);
    rec.state = dto::TxnRecordState::Committed;
    return _endHelper(rec);
}

seastar::future<Status> TxnManager::_abort(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to Abort for {}", rec);
    rec.state = dto::TxnRecordState::Aborted;
    return _endHelper(rec);
}

seastar::future<Status> TxnManager::_endHelper(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Processing END for {}", rec);

    auto timeout = (10s + _config.writeTimeout() * rec.writeRanges.size()) / _config.finalizeBatchSize();

    if (rec.syncFinalize) {
        return _finalizeTransaction(rec, FastDeadline(timeout));
    }

    // we're doing async finalize. enqueue in background tasks
    _addBgTask(rec,
        [this, &rec, timeout] {
            // this is only used for extra delay during testing. It is a txn end option
            if (nsec(rec.timeToFinalize).count() > 0) {
                return seastar::sleep(rec.timeToFinalize)
                    .then([this, &rec, timeout] {
                        return _finalizeTransaction(rec, FastDeadline(timeout)).discard_result();
                    });
            }
            return _finalizeTransaction(rec, FastDeadline(timeout)).discard_result();
        });

    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

seastar::future<Status> TxnManager::_finalizedPIP(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to FinalizedPIP for {}", rec);
    // set state
    rec.state = dto::TxnRecordState::FinalizedPIP;

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
                    _transactions.erase(rec.mtr.timestamp);
                    return seastar::make_ready_future();
                });
        });
    return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
}

// This helper generates the finalization requests for the given txn record
auto _genFinalizeRequests(TxnRecord& rec) {
    std::deque<std::tuple<dto::K23SITxnFinalizeRequest, dto::KeyRangeVersion>> requests;
    const auto endAction = rec.state == dto::TxnRecordState::Committed ? dto::EndAction::Commit : dto::EndAction::Abort;
    for (auto& [coll, rangeSet]: rec.writeRanges) {
        for (auto& krv: rangeSet) {
            requests.emplace_back(
            dto::K23SITxnFinalizeRequest{
                .pvid=krv.pvid,
                .collectionName=coll,
                .txnTimestamp=rec.mtr.timestamp,
                .action=endAction
            },
            krv);
        }
    }
    rec.writeRanges.clear();
    return requests;
}

void TxnManager::_genFinalizeReqsAfterPMAPUpdate(dto::K23SITxnFinalizeRequest&& failedReq,
    std::deque<std::tuple<dto::K23SITxnFinalizeRequest, dto::KeyRangeVersion>>& requests,
    dto::KeyRangeVersion&& krv) {
    // generate and queue up requests for all ranges
    auto it = _cpo.collections.find(failedReq.collectionName);
    if (it == _cpo.collections.end()) {
        K2LOG_W(log::skvsvr, "Collection not found: {}", failedReq.collectionName);
        requests.emplace_back(std::move(failedReq), std::move(krv));
        return;
    }
    for (auto& part: it->second->getAllPartitions()) {
        if (krv.startKey > part.keyRangeV.endKey) continue;
        if (krv.endKey < part.keyRangeV.startKey) break; // done
        K2LOG_D(log::skvsvr, "Remapped partition: {} --> {}", krv, part.keyRangeV);
        requests.emplace_back(
            dto::K23SITxnFinalizeRequest{
                .pvid=part.keyRangeV.pvid,
                .collectionName = failedReq.collectionName,
                .txnTimestamp = failedReq.txnTimestamp,
                .action = failedReq.action},
            part.keyRangeV);
    }
}

seastar::future<Status> TxnManager::_finalizeTransaction(TxnRecord& rec, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "Finalizing {}", rec);
    //TODO we need to keep trying to finalize in cases of failures.
    // this needs to be done in a rate-limited fashion.
    // For now, we just try some configurable number of times and give up
    return seastar::do_with((uint64_t)0, _genFinalizeRequests(rec),
        [this, &rec, deadline] (auto& batchStart, auto& requests) {
        return seastar::do_until(
            [this, &requests, &batchStart] { return batchStart >= requests.size(); },
            [this, &rec, &batchStart, deadline, &requests] {
                auto batchEnd = batchStart + std::min(_config.finalizeBatchSize(), requests.size() - batchStart);
                K2LOG_D(log::skvsvr, "Finalizing batch {}-{}/{}", batchStart, batchEnd, requests.size());
                return seastar::parallel_for_each(boost::irange(batchStart, batchEnd),
                [this, &rec, deadline, &requests] (auto idx) {
                    auto& [request, krv] = requests[idx];
                    K2LOG_D(log::skvsvr, "Finalizing req={}", request);
                    return _cpo.partitionRequestByPVID<dto::K23SITxnFinalizeRequest,
                                                dto::K23SITxnFinalizeResponse,
                                                dto::Verbs::K23SI_TXN_FINALIZE>
                    (deadline, request, _config.finalizeRetries())
                    .then([this, idx, &requests](auto&& responsePair) {
                        auto& [status, response] = responsePair;
                        auto& [request, krv] = requests[idx];

                        K2LOG_D(log::skvsvr, "Request {} completed with status {}, in krv {}", request, status, krv);
                        if (status == Statuses::S410_Gone) {
                            K2LOG_D(log::skvsvr, "Detected partition remapping for range {} in request: {}", krv, request);
                            // generate new requests and try again
                            _genFinalizeReqsAfterPMAPUpdate(std::move(request), requests, std::move(krv));
                            return seastar::make_ready_future<>();
                        }
                        if (!status.is2xxOK()) {
                            if (status != dto::K23SIStatus::KeyNotFound) {
                                K2LOG_E(log::skvsvr, "Finalize request did not succeed for {}, status={}", request, status);
                                // errors other than KeyNotFound need to be retried.
                                // however KeyNotFound is acceptable since it may simply indicate
                                // that the client's transaction had a failed write
                                return seastar::make_exception_future<>(std::runtime_error(fmt::format("finalize request failed after retrying due to {}", status)));
                            }
                            else {
                                K2LOG_W(log::skvsvr, "Finalize request did not find txn for {}, status={}. Assume this is a retry and skip range", request, status);
                                return seastar::make_ready_future<>();
                            }
                        }

                        K2LOG_D(log::skvsvr, "Finalize request succeeded for {}", request);
                        return seastar::make_ready_future<>();
                    });
                })
                .finally([&batchStart, batchEnd] {
                    batchStart = batchEnd + 1;
                }); // parallel_for_each
            }); // do_until
    })
    .then_wrapped([this, &rec] (auto&& fut) {
        if (fut.failed()) {
            K2LOG_W_EXC(log::skvsvr, fut.get_exception(), "Unable to finalize txn {}. Leaving in memory", rec);
            return seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
        }
        K2LOG_D(log::skvsvr, "finalize completed for: {}", rec);
        return _onAction(TxnRecord::Action::onFinalizeComplete, rec);
    });
}

}  // namespace k2
