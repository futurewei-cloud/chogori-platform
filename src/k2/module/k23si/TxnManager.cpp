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
    // unlink if necessary
    rec.unlinkBG(_bgTasks);
    _bgTasks.push_back(rec);

    rec.bgTaskFut = rec.bgTaskFut.then(std::forward<Func>(func));
}

void TxnManager::_addBgTaskFuture(TxnRecord& rec, seastar::future<>&& fut) {
    _addBgTask(rec, [fut=std::move(fut)] () mutable {
        return std::move(fut);
    });
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
void TxnRecord::unlinkBG(BGList& bglist) {
    if (bgTaskLink.is_linked()) {
        bglist.erase(bglist.iterator_to(*this));
    }
}

TxnManager::~TxnManager() {
    K2LOG_I(log::skvsvr, "dtor for cname={}", _collectionName);
    _hblist.clear();
    _rwlist.clear();
    _bgTasks.clear();
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

    _hbTimer.set_callback([this] {
        K2LOG_D(log::skvsvr, "txn manager check hb");
        _hbTask = _hbTask.then([this] {
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
                        return onAction(TxnRecord::Action::onHeartbeatExpire, tr.txnId);
                    }
                    else if (!_rwlist.empty() && _rwlist.front().rwExpiry.compareCertain(_retentionTs) <= 0) {
                        auto& tr = _rwlist.front();
                        K2LOG_W(log::skvsvr, "rw expired on: {}", tr);
                        _rwlist.pop_front();
                        return onAction(TxnRecord::Action::onRetentionWindowExpire, tr.txnId);
                    }
                    K2LOG_E(log::skvsvr, "Heartbeat processing failure - expected to find either hb or rw expired item but none found");
                    return seastar::make_ready_future();
            })
            .then([this] {
                _hbTimer.arm(_hbDeadline);
            })
            .handle_exception([] (auto exc){
                K2LOG_W_EXC(log::skvsvr, exc, "caught exception while checking hb/rw expiration");
                return seastar::make_ready_future();
            });
        });
    });
    _hbTimer.arm(_hbDeadline);

    return seastar::make_ready_future();
}

seastar::future<> TxnManager::gracefulStop() {
    K2LOG_I(log::skvsvr, "stopping txn mgr for coll={}", _collectionName);
    _stopping = true;
    _hbTimer.cancel();
    return _hbTask.then([this] {
        K2LOG_I(log::skvsvr, "hb stopped. stopping {} bg tasks", _bgTasks.size());
        std::vector<seastar::future<>> _bgFuts;
        for (auto& txn: _bgTasks) {
            K2LOG_I(log::skvsvr, "Waiting for bg task in {}", txn);
            _bgFuts.push_back(std::move(txn.bgTaskFut));
        }
        return seastar::when_all_succeed(_bgFuts.begin(), _bgFuts.end()).discard_result()
        .then([]{
            K2LOG_I(log::skvsvr, "stopped");
        })
        .handle_exception([](auto exc) {
            K2LOG_W_EXC(log::skvsvr, exc, "caught exception on stop");
            return seastar::make_ready_future();
        });
    });
}

void TxnManager::updateRetentionTimestamp(dto::Timestamp rts) {
    K2LOG_D(log::skvsvr, "retention ts now={}", rts)
    _retentionTs = rts;
}

TxnRecord* TxnManager::getTxnRecordNoCreate(const dto::TxnId& txnId) {
    auto it = _transactions.find(txnId);
    if (it != _transactions.end()) {
        K2LOG_D(log::skvsvr, "found existing record: {}", it->second);
        return &(it->second);
    }

    K2LOG_D(log::skvsvr, "Txn record not found for {}", txnId);
    return nullptr;
}

TxnRecord& TxnManager::getTxnRecord(const dto::TxnId& txnId) {
    auto it = _transactions.find(txnId);
    if (it != _transactions.end()) {
        K2LOG_D(log::skvsvr, "found existing record: {}", it->second);
        return it->second;
    }
    K2LOG_D(log::skvsvr, "Txn record not found for {}. creating one", txnId);
    return _createRecord(txnId);
}

TxnRecord& TxnManager::getTxnRecord(dto::TxnId&& txnId) {
    auto it = _transactions.find(txnId);
    if (it != _transactions.end()) {
        K2LOG_D(log::skvsvr, "found existing record for {}: {}", txnId, it->second);
        return it->second;
    }
    K2LOG_D(log::skvsvr, "Txn record not found for {}. creating one", txnId);
    return _createRecord(std::move(txnId));
}

TxnRecord& TxnManager::_createRecord(dto::TxnId txnId) {
    // we don't persist the record on create. If we have a sudden failure, we'd just abort the transaction when
    // it comes to commit.
    auto it = _transactions.insert({std::move(txnId), TxnRecord{}});
    if (it.second) {
        TxnRecord& rec = it.first->second;
        rec.txnId = it.first->first;
        rec.state = dto::TxnRecordState::Created;
        rec.rwExpiry = txnId.mtr.timestamp;
        rec.hbExpiry = CachedSteadyClock::now() + 2*_hbDeadline;

        _hblist.push_back(rec);
        _rwlist.push_back(rec);
    }
    K2LOG_D(log::skvsvr, "created new txn record: {}", it.first->second);
    return it.first->second;
}

seastar::future<> TxnManager::onAction(TxnRecord::Action action, dto::TxnId txnId) {
    // This method's responsibility is to execute valid state transitions.
    TxnRecord& rec = getTxnRecord(std::move(txnId));
    auto state = rec.state;
    K2LOG_D(log::skvsvr, "Processing action {}, for state {}, in txn {}", action, state, rec);
    switch (state) {
        case dto::TxnRecordState::Created:
            // We did not have a transaction record and it was just created
            switch (action) {
                case TxnRecord::Action::onCreate:
                    return _inProgress(rec);
                case TxnRecord::Action::onRetentionWindowExpire:
                case TxnRecord::Action::onForceAbort:
                    return _forceAborted(rec);
                case TxnRecord::Action::onHeartbeat: // illegal - create a ForceAborted entry and wait for End
                    return _forceAborted(rec)
                        .then([]{
                            // respond with failure since we had to force abort but were asked to heartbeat
                            return seastar::make_exception_future(ClientError("cannot heartbeat transaction since it doesn't exist"));
                        });
                case TxnRecord::Action::onEndCommit:  // create an entry in Aborted state so that it can be finalized
                    return _end(rec, dto::TxnRecordState::Aborted)
                        .then([] {
                            // respond with failure since we had to abort but were asked to commit
                            return seastar::make_exception_future(ClientError("cannot commit transaction since it has been aborted"));
                        });
                case TxnRecord::Action::onEndAbort:  // create an entry in Aborted state so that it can be finalized
                    return _end(rec, dto::TxnRecordState::Aborted);
                case TxnRecord::Action::onHeartbeatExpire:        // internal error - must have a TR
                case TxnRecord::Action::onFinalizeComplete:       // internal error - must have a TR
                default: // anything else we just count as internal error
                    K2LOG_E(log::skvsvr, "Invalid transition {} for txnid: {}, in state {}", action, txnId, state);
                    return seastar::make_exception_future(ServerError("invalid transition"));
            };
        case dto::TxnRecordState::InProgress:
            switch (action) {
                case TxnRecord::Action::onCreate: // no-op - stay in same state
                    return _inProgress(rec);
                case TxnRecord::Action::onHeartbeat:
                    return _heartbeat(rec);
                case TxnRecord::Action::onEndCommit:
                    return _end(rec, dto::TxnRecordState::Committed);
                case TxnRecord::Action::onEndAbort:
                    return _end(rec, dto::TxnRecordState::Aborted);
                case TxnRecord::Action::onForceAbort:             // asked to force-abort (e.g. on PUSH)
                case TxnRecord::Action::onRetentionWindowExpire:  // we've had this transaction for too long
                case TxnRecord::Action::onHeartbeatExpire:        // originator didn't hearbeat on time
                    return _forceAborted(rec);
                case TxnRecord::Action::onFinalizeComplete:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}, in state: {}", txnId, state);
                    return seastar::make_exception_future(ServerError("invalid transition"));
            };
        case dto::TxnRecordState::ForceAborted:
            switch (action) {
                case TxnRecord::Action::onCreate: // this has been aborted already. Signal the client to issue endAbort
                    return seastar::make_exception_future(ClientError("cannot create transaction since it has been force-aborted"));
                case TxnRecord::Action::onForceAbort:  // no-op
                    return seastar::make_ready_future();
                case TxnRecord::Action::onRetentionWindowExpire:
                    return _deleted(rec);
                case TxnRecord::Action::onEndCommit:
                    return _end(rec, dto::TxnRecordState::Aborted)
                        .then([] {
                            // respond with failure since we had to abort but were asked to commit
                            return seastar::make_exception_future(ClientError("cannot commit transaction since it has been force-aborted"));
                        });
                case TxnRecord::Action::onEndAbort:
                    return _end(rec, dto::TxnRecordState::Aborted);
                case TxnRecord::Action::onHeartbeat: // signal client to abort
                    return seastar::make_exception_future(ClientError("cannot heartbeat transaction since it has been force-aborted"));
                case TxnRecord::Action::onFinalizeComplete:
                case TxnRecord::Action::onHeartbeatExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", txnId);
                    return seastar::make_exception_future(ServerError("invalid transition"));
            };
        case dto::TxnRecordState::Aborted:
            switch (action) {
                case TxnRecord::Action::onCreate: // signal client to abort
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                    return seastar::make_ready_future();  // allow as no-op
                case TxnRecord::Action::onEndCommit:
                    return seastar::make_exception_future(ClientError("cannot commit transaction since it has been aborted"));
                case TxnRecord::Action::onEndAbort: // accept this to be re-entrant
                    return seastar::make_ready_future();
                case TxnRecord::Action::onFinalizeComplete: // on to deleting this record
                    return _deleted(rec);
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", txnId);
                    return seastar::make_exception_future(ServerError("invalid transition"));
            };
        case dto::TxnRecordState::Committed:
            switch (action) {
                case TxnRecord::Action::onCreate: // signal client to abort
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                    return seastar::make_ready_future();  // allow as no-op
                case TxnRecord::Action::onEndAbort:
                    return seastar::make_exception_future(ClientError("cannot abort transaction since it has been committed"));
                case TxnRecord::Action::onEndCommit: // accept this to be re-entrant
                    return seastar::make_ready_future();
                case TxnRecord::Action::onFinalizeComplete:
                    return _deleted(rec);
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", txnId);
                    return seastar::make_exception_future(ServerError("invalid transition"));
            };
        case dto::TxnRecordState::Deleted:
            switch (action) {
                case TxnRecord::Action::onEndAbort:
                    return seastar::make_ready_future();  // allow as no-op
                case TxnRecord::Action::onCreate:
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                case TxnRecord::Action::onEndCommit: // accept this to be re-entrant
                case TxnRecord::Action::onFinalizeComplete:
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2LOG_E(log::skvsvr, "Invalid transition for txnid: {}", txnId);
                    return seastar::make_exception_future(ServerError("invalid transition"));
            };
        default:
            K2LOG_E(log::skvsvr, "Invalid record state ({}), for action: {}, in txnid: {}", state, action, txnId);
            return seastar::make_exception_future(ServerError("invalid record state"));
    }
}

seastar::future<> TxnManager::_inProgress(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to inProgress for {}", rec);
    // set state
    rec.state = dto::TxnRecordState::InProgress;
    // manage hb expiry: we only come here immediately after Created which sets HB
    // manage rw expiry: same as hb
    // persist if needed: no need - in case of failures, we'll just abort
    return seastar::make_ready_future();
}

seastar::future<> TxnManager::_forceAborted(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to forceAborted for {}", rec);
    // set state
    rec.state = dto::TxnRecordState::ForceAborted;
    // there is no longer a heartbeat expectation
    rec.unlinkHB(_hblist);

    // we still want to keep the record inked in the retention window since we want to take action if it goes past the RWE

    // append to WAL
    _addBgTaskFuture(rec, _persistence->append(rec));
    return seastar::make_ready_future();
}

seastar::future<> TxnManager::_end(TxnRecord& rec, dto::TxnRecordState state) {
    K2LOG_D(log::skvsvr, "Setting state to {}, for {}", state, rec);
    // set state
    rec.state = state;
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);

    _addBgTaskFuture(rec, _persistence->append(rec));

    return _persistence->flush().then([this, &rec] (auto&& flushStatus) {
        if (!flushStatus.is2xxOK()) {
            return seastar::make_exception_future(ServerError("persistence flush failed"));
        }

        auto timeout = (10s + _config.writeTimeout() * rec.writeKeys.size()) / _config.finalizeBatchSize();

        if (rec.syncFinalize) {
            // append to WAL
            _addBgTaskFuture(rec, _persistence->append(rec));

            return _finalizeTransaction(rec, FastDeadline(timeout));
        }
        else {
            // enqueue in background tasks
            rec.bgTaskFut = rec.bgTaskFut
                .then([&rec] {
                    return seastar::sleep(rec.timeToFinalize);
                })
                .then([this, &rec, timeout]() {
                    return _finalizeTransaction(rec, FastDeadline(timeout));
                });
            // append to WAL
            _addBgTaskFuture(rec, _persistence->append(rec));
            return seastar::make_ready_future();
        }
    });
}

seastar::future<> TxnManager::_deleted(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Setting status to deleted for {}", rec);
    // set state
    rec.state = dto::TxnRecordState::Deleted;
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);

    // append to WAL
    _addBgTaskFuture(rec,
        _persistence->append(rec)
        .then([this, &rec] {
            // once flushed, erase from memory
            K2LOG_D(log::skvsvr, "Erasing txn record: {}", rec);
            rec.unlinkBG(_bgTasks);
            rec.unlinkRW(_rwlist);
            rec.unlinkHB(_hblist);
            _transactions.erase(rec.txnId);
        })
    );

    return seastar::make_ready_future();
}

seastar::future<> TxnManager::_heartbeat(TxnRecord& rec) {
    K2LOG_D(log::skvsvr, "Processing heartbeat for {}", rec);
    // set state: no change
    // manage hb expiry
    rec.unlinkHB(_hblist);
    rec.hbExpiry = CachedSteadyClock::now() + 2*_hbDeadline;
    _hblist.push_back(rec);
    // manage rw expiry: no change
    // persist if needed: no need
    return seastar::make_ready_future();
}

seastar::future<> TxnManager::_finalizeTransaction(TxnRecord& rec, FastDeadline deadline) {
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
                    request.action = rec.state == dto::TxnRecordState::Committed ? dto::EndAction::Commit : dto::EndAction::Abort;
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
                                return seastar::make_exception_future<>(TxnManager::ServerError("finalize request failed after retrying"));
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
    .then([this, &rec] {
        K2LOG_D(log::skvsvr, "finalize completed for: {}", rec);
        return onAction(TxnRecord::Action::onFinalizeComplete, rec.txnId);
    });
}

}  // namespace k2
