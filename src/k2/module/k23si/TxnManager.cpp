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

seastar::future<> TxnManager::start(const String& collectionName, dto::Timestamp rts, Duration retentionPeriod, Duration hbDeadline) {
    _collectionName = collectionName;
    _retentionPeriod = retentionPeriod;
    _hbDeadline = hbDeadline;
    updateRetentionTimestamp(rts);
    _hbTimer.set_callback([this] {
        K2DEBUG("txn manager check hb");
        _hbTask = _hbTask.then([this] {
            // refresh the clock
            auto now = CachedSteadyClock::now(true);
            uint64_t dispatched = 0;
            return seastar::do_with(std::move(dispatched), [this, now](uint64_t& dispatched) {
                return seastar::do_until(
                    [this, now, &dispatched] {
                        auto cantExpire = dispatched >= _config.maxHBExpireCount();
                        auto noHB = _hblist.empty() || _hblist.front().hbExpiry > now;
                        auto noRW = _rwlist.empty() || _rwlist.front().rwExpiry > _tsonow;
                        return cantExpire || (noHB && noRW);
                    },
                    [this, &dispatched, now] {
                        dispatched++;
                        if (!_hblist.empty() && _hblist.front().hbExpiry < now) {
                            auto& tr = _hblist.front();
                            _hblist.pop_front();
                            return onAction(TxnRecord::Action::onHeartbeatExpire, tr.txnId);
                        }
                        else if (!_rwlist.empty() && _rwlist.front().rwExpiry < _tsonow) {
                            auto& tr = _rwlist.front();
                            _rwlist.pop_front();
                            return onAction(TxnRecord::Action::onRetentionWindowExpire, tr.txnId);
                        }
                        return seastar::make_exception_future<>(ServerError());
                    });
            })
            .then([this] {
                _hbTimer.arm(_hbDeadline);
            });
        });
    });
    _hbTimer.arm(_hbDeadline);
    // TODO recover transaction state
    return _persistence.makeCall(FastDeadline(10s));
}

seastar::future<> TxnManager::stop() {
    _stopping = true;
    _hbTimer.cancel();
    return std::move(_hbTask).then([this]{
        return seastar::parallel_for_each(_bgTasks.begin(), _bgTasks.end(), [](auto& txn){
            return std::move(txn.bgTaskFut);
        }).discard_result();
    });
}

void TxnManager::updateRetentionTimestamp(dto::Timestamp rts) {
    _tsonow = rts.toTimePoint();
}

TxnRecord& TxnManager::getTxnRecord(const TxnId& txnId) {
    auto it = _transactions.find(txnId);
    if (it != _transactions.end()) {
        return it->second;
    }
    return _createRecord(txnId);
}

TxnRecord& TxnManager::getTxnRecord(TxnId&& txnId) {
    auto it = _transactions.find(txnId);
    if (it != _transactions.end()) {
        return it->second;
    }
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
        rec.rwExpiry = _tsonow + _retentionPeriod;
        rec.hbExpiry = _tsonow + _hbDeadline;

        _hblist.push_back(rec);
        _rwlist.push_back(rec);
    }
    return it.first->second;
}

seastar::future<> TxnManager::onAction(TxnRecord::Action action, TxnId txnId) {
    // This method's responsibility is to execute valid state transitions.
    TxnRecord& rec = getTxnRecord(std::move(txnId));
    auto state = rec.state;
    K2DEBUG("Processing action " << action << ", for state " << state);
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
    // set state
    rec.state = TxnRecord::State::InProgress;
    // manage hb expiry: we only come here immediately after Created which sets HB
    // manage rw expiry: same as hb
    // persist if needed: no need - in case of failures, we'll just abort
    return seastar::make_ready_future();
}

seastar::future<> TxnManager::_forceAborted(TxnRecord& rec) {
    // set state
    rec.state = TxnRecord::State::ForceAborted;
    // manage hb expiry
    _hblist.erase(_hblist.iterator_to(rec));
    // manage rw expiry: we want to track expiration on retention window
    // persist if needed
    return _persistence.makeCall(FastDeadline(10s));
}

seastar::future<> TxnManager::_aborted(TxnRecord& rec) {
    // set state
    rec.state = TxnRecord::State::Aborted;
    // manage hb expiry
    _hblist.erase(_hblist.iterator_to(rec));
    // manage rw expiry
    _rwlist.erase(_rwlist.iterator_to(rec));
    // queue up background task for finalizing
    rec.bgTaskFut = rec.bgTaskFut.then([this, &rec] () mutable{
        return _finalizeTransaction(rec, FastDeadline(_config.writeTimeout()));
    });
    _bgTasks.push_back(rec);
    // persist if needed
    return _persistence.makeCall(FastDeadline(10s));
}

seastar::future<> TxnManager::_committed(TxnRecord& rec) {
    // set state
    rec.state = TxnRecord::State::Committed;
    // manage hb expiry
    _hblist.erase(_hblist.iterator_to(rec));
    // manage rw expiry
    _rwlist.erase(_rwlist.iterator_to(rec));
    // queue up background task for finalizing
    rec.bgTaskFut = rec.bgTaskFut.then([this, &rec]() mutable {
        return _finalizeTransaction(rec, FastDeadline(_config.writeTimeout()));
    });
    _bgTasks.push_back(rec);
    // persist if needed
    return _persistence.makeCall(FastDeadline(10s));
}

seastar::future<> TxnManager::_deleted(TxnRecord& rec) {
    // set state
    rec.state = TxnRecord::State::Deleted;
    // manage hb expiry
    _hblist.erase(_hblist.iterator_to(rec));
    // manage rw expiry
    _rwlist.erase(_rwlist.iterator_to(rec));
    // persist if needed

    return _persistence.makeCall(FastDeadline(10s));
}

seastar::future<> TxnManager::_heartbeat(TxnRecord& rec) {
    // set state: no change
    // manage hb expiry
    _hblist.erase(_hblist.iterator_to(rec));
    rec.hbExpiry = _tsonow + _hbDeadline;
    _hblist.push_back(rec);
    // manage rw expiry: no change
    // persist if needed: no need
    return seastar::make_ready_future();
}

seastar::future<> TxnManager::_finalizeTransaction(TxnRecord& rec, FastDeadline deadline) {
    //TODO we need to keep trying to finalize in cases of failures.
    // this needs to be done in a rate-limited fashion. For now, we just try some configurable number of times and give up
    return seastar::parallel_for_each(rec.writeKeys.begin(), rec.writeKeys.end(), [&rec, this, deadline](dto::Key& key) {
        dto::K23SITxnFinalizeRequest request{};
        request.key = std::move(key);
        request.collectionName = _collectionName;
        request.mtr = rec.txnId.mtr;
        request.trh = rec.txnId.trh;
        request.action = rec.state == TxnRecord::State::Committed ? dto::EndAction::Commit : dto::EndAction::Abort;
        return seastar::do_with(std::move(request), [&rec, this, deadline] (auto& request) {
            return _cpo.PartitionRequest<dto::K23SITxnFinalizeRequest,
                                         dto::K23SITxnFinalizeResponse,
                                         dto::Verbs::K23SI_TXN_FINALIZE>
            (deadline, request, _config.finalizeRetries())
            .then([](auto&& responsePair) {
                auto& [status, response] = responsePair;
                if (status != dto::K23SIStatus::OK()) {
                    return seastar::make_exception_future<>(TxnManager::ServerError());
                }
                return seastar::make_ready_future<>();
            });
        });
    })
    .then([this, &rec] {
        return onAction(TxnRecord::Action::onFinalizeComplete, rec.txnId);
    });
}

}  // namespace k2
