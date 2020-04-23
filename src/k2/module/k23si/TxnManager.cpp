#include "TxnManager.h"

namespace k2 {

size_t TxnId::hash() const {
    return trh.hash() + mtr.hash();
}

bool TxnId::operator==(const TxnId& o) const{
    return trh == o.trh && mtr == o.mtr;
}

bool TxnId::operator!=(const TxnId& o) const{
    return !operator==(o);
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
    K2DEBUG("dtor");
    _hblist.clear();
    _rwlist.clear();
    _bgTasks.clear();
    for (auto& [key, trec]: _transactions) {
        K2WARN("Shutdown dropping transaction: " << trec);
    }
}

seastar::future<> TxnManager::start(const String& collectionName, dto::Timestamp rts, Duration hbDeadline) {
    K2DEBUG("start");
    _collectionName = collectionName;
    _hbDeadline = hbDeadline;
    updateRetentionTimestamp(rts);
    _hbTimer.set_callback([this] {
        K2DEBUG("txn manager check hb");
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
                        K2WARN("heartbeat expired on: " << tr);
                        _hblist.pop_front();
                        return onAction(TxnRecord::Action::onHeartbeatExpire, tr.txnId);
                    }
                    else if (!_rwlist.empty() && _rwlist.front().rwExpiry.compareCertain(_retentionTs) <= 0) {
                        auto& tr = _rwlist.front();
                        K2WARN("rw expired on: " << tr);
                        _rwlist.pop_front();
                        return onAction(TxnRecord::Action::onRetentionWindowExpire, tr.txnId);
                    }
                    K2ERROR("Heartbeat processing failure - expected to find either hb or rw expired item but none found");
                    return seastar::make_ready_future();
            })
            .then([this] {
                _hbTimer.arm(_hbDeadline);
            })
            .handle_exception([] (auto exc){
                K2ERROR_EXC("caught exception while checking hb/rw expiration", exc);
                return seastar::make_ready_future();
            });
        });
    });
    _hbTimer.arm(_hbDeadline);
    // TODO recover transaction state
    return _persistence.makeCall(dto::K23SI_PersistenceRecoveryRequest{}, FastDeadline(10s));
}

seastar::future<> TxnManager::stop() {
    K2DEBUG("stop");
    _stopping = true;
    _hbTimer.cancel();
    return std::move(_hbTask).then([this]{
        K2DEBUG("hb stopped. stopping " << _bgTasks.size() << " bg tasks");
        return seastar::do_for_each(_bgTasks.begin(), _bgTasks.end(), [](auto& txn){
            K2DEBUG("waiting for bg task on " << txn);
            return std::move(txn.bgTaskFut);
        }).discard_result().then([]{K2DEBUG("stopped");}).handle_exception([](auto exc) {
            K2ERROR_EXC("caught exception on stop", exc);
            return seastar::make_ready_future();
        });
    });
}

void TxnManager::updateRetentionTimestamp(dto::Timestamp rts) {
    K2DEBUG("retention ts now=" << rts)
    _retentionTs = rts;
}

TxnRecord& TxnManager::getTxnRecord(const TxnId& txnId) {
    auto it = _transactions.find(txnId);
    if (it != _transactions.end()) {
        K2DEBUG("found existing record: " << it->second);
        return it->second;
    }
    K2DEBUG("Txn record not found. creating one");
    return _createRecord(txnId);
}

TxnRecord& TxnManager::getTxnRecord(TxnId&& txnId) {
    auto it = _transactions.find(txnId);
    if (it != _transactions.end()) {
        K2DEBUG("found existing record: " << it->second);
        return it->second;
    }
    K2DEBUG("Txn record not found. creating one");
    return _createRecord(std::move(txnId));
}

TxnRecord& TxnManager::_createRecord(TxnId txnId) {
    // we don't persist the record on create. If we have a sudden failure, we'd just abort the transaction when
    // it comes to commit.
    auto it = _transactions.insert({std::move(txnId), TxnRecord{}});
    if (it.second) {
        TxnRecord& rec = it.first->second;
        rec.txnId = it.first->first;
        rec.state = TxnRecord::State::Created;
        rec.rwExpiry = txnId.mtr.timestamp;
        rec.hbExpiry = CachedSteadyClock::now() + 2*_hbDeadline;

        _hblist.push_back(rec);
        _rwlist.push_back(rec);
    }
    K2DEBUG("created new txn record: " << it.first->second);
    return it.first->second;
}

seastar::future<> TxnManager::onAction(TxnRecord::Action action, TxnId txnId) {
    // This method's responsibility is to execute valid state transitions.
    TxnRecord& rec = getTxnRecord(std::move(txnId));
    auto state = rec.state;
    K2DEBUG("Processing action " << action << ", for state " << state << ", in txn " << rec);
    if (action != TxnRecord::Action::onHeartbeat || state != TxnRecord::State::InProgress) {
        K2INFO("Processing action " << action << ", for state " << state << ", in txn " << rec);
    }
    switch (state) {
        case TxnRecord::State::Created:
            // We did not have a transaction record and it was just created
            switch (action) {
                case TxnRecord::Action::onCreate:
                    return _inProgress(rec);
                case TxnRecord::Action::onForceAbort:
                    return _forceAborted(rec);
                case TxnRecord::Action::onHeartbeat: // illegal - create a ForceAborted entry and wait for End
                    return _forceAborted(rec)
                        .then([]{
                            // respond with failure since we had to force abort but were asked to heartbeat
                            return seastar::make_exception_future(ClientError());
                        });
                case TxnRecord::Action::onEndCommit:  // create an entry in Aborted state so that it can be finalized
                    return _aborted(rec)
                        .then([] {
                            // respond with failure since we had to abort but were asked to commit
                            return seastar::make_exception_future(ClientError());
                        });
                case TxnRecord::Action::onEndAbort:  // create an entry in Aborted state so that it can be finalized
                    return _aborted(rec);
                case TxnRecord::Action::onHeartbeatExpire:        // internal error - must have a TR
                case TxnRecord::Action::onRetentionWindowExpire:  // internal error - must have a TR
                case TxnRecord::Action::onFinalizeComplete:       // internal error - must have a TR
                default: // anything else we just count as internal error
                    K2ERROR("Invalid transition for txnid: " << txnId << ", in state: " << state);
                    return seastar::make_exception_future(ServerError());
            };
        case TxnRecord::State::InProgress:
            switch (action) {
                case TxnRecord::Action::onCreate: // no-op - stay in same state
                    return _inProgress(rec);
                case TxnRecord::Action::onHeartbeat:
                    return _heartbeat(rec);
                case TxnRecord::Action::onEndCommit:
                    return _committed(rec);
                case TxnRecord::Action::onEndAbort:
                    return _aborted(rec);
                case TxnRecord::Action::onForceAbort:             // asked to force-abort (e.g. on PUSH)
                case TxnRecord::Action::onRetentionWindowExpire:  // we've had this transaction for too long
                case TxnRecord::Action::onHeartbeatExpire:        // originator didn't hearbeat on time
                    return _forceAborted(rec);
                case TxnRecord::Action::onFinalizeComplete:
                default:
                    K2ERROR("Invalid transition for txnid: " << txnId << ", in state: " << state);
                    return seastar::make_exception_future(ServerError());
            };
        case TxnRecord::State::ForceAborted:
            switch (action) {
                case TxnRecord::Action::onCreate: // this has been aborted already. Signal the client to issue endAbort
                    return seastar::make_exception_future(ClientError());
                case TxnRecord::Action::onForceAbort:  // no-op
                    return seastar::make_ready_future();
                case TxnRecord::Action::onRetentionWindowExpire:
                    return _deleted(rec);
                case TxnRecord::Action::onEndCommit:
                    return _aborted(rec)
                        .then([] {
                            // respond with failure since we had to abort but were asked to commit
                            return seastar::make_exception_future(ClientError());
                        });
                case TxnRecord::Action::onEndAbort:
                    return _aborted(rec);
                case TxnRecord::Action::onHeartbeat: // signal client to abort
                    return seastar::make_exception_future(ClientError());
                case TxnRecord::Action::onFinalizeComplete:
                case TxnRecord::Action::onHeartbeatExpire:
                default:
                    K2ERROR("Invalid transition for txnid: " << txnId);
                    return seastar::make_exception_future(ServerError());
            };
        case TxnRecord::State::Aborted:
            switch (action) {
                case TxnRecord::Action::onCreate: // signal client to abort
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                case TxnRecord::Action::onEndCommit:
                    return seastar::make_exception_future(ClientError());
                case TxnRecord::Action::onEndAbort: // accept this to be re-entrant
                    return seastar::make_ready_future();
                case TxnRecord::Action::onFinalizeComplete: // on to deleting this record
                    return _deleted(rec);
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2ERROR("Invalid transition for txnid: " << txnId);
                    return seastar::make_exception_future(ServerError());
            };
        case TxnRecord::State::Committed:
            switch (action) {
                case TxnRecord::Action::onCreate: // signal client to abort
                case TxnRecord::Action::onForceAbort:
                case TxnRecord::Action::onHeartbeat:
                case TxnRecord::Action::onEndAbort:
                    return seastar::make_exception_future(ClientError());
                case TxnRecord::Action::onEndCommit: // accept this to be re-entrant
                    return seastar::make_ready_future();
                case TxnRecord::Action::onFinalizeComplete:
                    return _deleted(rec);
                case TxnRecord::Action::onHeartbeatExpire:
                case TxnRecord::Action::onRetentionWindowExpire:
                default:
                    K2ERROR("Invalid transition for txnid: " << txnId);
                    return seastar::make_exception_future(ServerError());
            };
        default:
            K2ERROR("Invalid record state (" << state << "), for txnid: " << txnId);
            return seastar::make_exception_future(ServerError());
    }
}

seastar::future<> TxnManager::_inProgress(TxnRecord& rec) {
    K2DEBUG("Setting status to inProgress for " << rec);
    // set state
    rec.state = TxnRecord::State::InProgress;
    // manage hb expiry: we only come here immediately after Created which sets HB
    // manage rw expiry: same as hb
    // persist if needed: no need - in case of failures, we'll just abort
    return seastar::make_ready_future();
}

seastar::future<> TxnManager::_forceAborted(TxnRecord& rec) {
    K2DEBUG("Setting status to forceAborted for " << rec);
    // set state
    rec.state = TxnRecord::State::ForceAborted;
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry: we want to track expiration on retention window
    // persist if needed
    return _persistence.makeCall(rec, FastDeadline(10s));
}

seastar::future<> TxnManager::_aborted(TxnRecord& rec) {
    K2DEBUG("Setting status to aborted for " << rec);
    // set state
    rec.state = TxnRecord::State::Aborted;
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);
    // queue up background task for finalizing
    _bgTasks.push_back(rec);
    rec.bgTaskFut = rec.bgTaskFut
    .then([] {
        return seastar::sleep(0us);
    })
    .then([this, &rec]() {
        // TODO Deadline based on transaction size
        return _finalizeTransaction(rec, FastDeadline(5s));
    });
    // persist if needed
    return _persistence.makeCall(rec, FastDeadline(10s));
}

seastar::future<> TxnManager::_committed(TxnRecord& rec) {
    K2DEBUG("Setting status to committed for " << rec);
    // set state
    rec.state = TxnRecord::State::Committed;
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);

    return _persistence.makeCall(rec, FastDeadline(10s))
    .then([&rec, this]{
        return _finalizeTransaction(rec, FastDeadline(5s  +  10ms * rec.writeKeys.size()));
    });
    /*
    // queue up background task for finalizing
    _bgTasks.push_back(rec);
    rec.bgTaskFut = rec.bgTaskFut
    .then([] {
        return seastar::sleep(0us);
    })
    .then([this, &rec]() {
        // TODO Deadline based on transaction size
        return _finalizeTransaction(rec, FastDeadline(5s));
    });
    // persist if needed
    return _persistence.makeCall(rec, FastDeadline(10s));
    */
}

seastar::future<> TxnManager::_deleted(TxnRecord& rec) {
    K2DEBUG("Setting status to deleted for " << rec);
    // set state
    rec.state = TxnRecord::State::Deleted;
    // manage hb expiry
    rec.unlinkHB(_hblist);
    // manage rw expiry
    rec.unlinkRW(_rwlist);
    // persist if needed

    return _persistence.makeCall(rec, FastDeadline(10s)).then([this, &rec]{
        rec.unlinkBG(_bgTasks);
        rec.unlinkRW(_rwlist);
        rec.unlinkHB(_hblist);
        _transactions.erase(rec.txnId);
    });
}

seastar::future<> TxnManager::_heartbeat(TxnRecord& rec) {
    K2DEBUG("Processing heartbeat for " << rec);
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
    K2INFO("Finalizing " << rec);
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
                    request.action = rec.state == TxnRecord::State::Committed ? dto::EndAction::Commit : dto::EndAction::Abort;
                    K2DEBUG("Finalizing req=" << request);
                    return seastar::do_with(std::move(request), [&rec, this, deadline](auto& request) {
                        return _cpo.PartitionRequest<dto::K23SITxnFinalizeRequest,
                                                    dto::K23SITxnFinalizeResponse,
                                                    dto::Verbs::K23SI_TXN_FINALIZE>
                        (deadline, request, _config.finalizeRetries())
                        .then([&request](auto&& responsePair) {
                            auto& [status, response] = responsePair;
                            if (!status.is2xxOK()) {
                                K2ERROR("Finalize request did not succeed for " << request << ", status=" << status);
                                return seastar::make_exception_future<>(TxnManager::ServerError());
                            }
                            K2DEBUG("Finalize request succeeded for " << request);
                            return seastar::make_ready_future<>();
                        }).finally([]{ K2DEBUG("finalize call finished");});
                    });
                }).then([&batchStart, &rec]{
                    K2DEBUG("Batch done, now at: " << batchStart << ", in " << rec);
                });
            }
        );
    })
    .then([this, &rec] {
        K2INFO("finalize completed for: " << rec);
        return onAction(TxnRecord::Action::onFinalizeComplete, rec.txnId);
    });
}

}  // namespace k2
