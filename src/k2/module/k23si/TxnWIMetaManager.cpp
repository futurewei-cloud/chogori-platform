/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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
#include "TxnWIMetaManager.h"
#include <k2/dto/K23SIInspect.h>

namespace k2 {

bool TxnWIMeta::isCommitted() {
    return finalizeAction == dto::EndAction::Commit;
}

bool TxnWIMeta::isAborted() {
    return finalizeAction == dto::EndAction::Abort;
}

bool TxnWIMeta::isInProgress() {
    return state == dto::TxnWIMetaState::InProgress || state == dto::TxnWIMetaState::InProgressPIP;
}

void TxnWIMeta::unlinkRW(RWList& rwlist) {
    if (rwLink.is_linked()) {
        rwlist.erase(rwlist.iterator_to(*this));
    }
}

template <typename Func>
void TxnWIMetaManager::_addBgTask(TxnWIMeta& rec, Func&& func) {
    K2LOG_D(log::skvsvr, "Adding background task");
    if (_stopping) {
        K2LOG_W(log::skvsvr, "Attempting to add a background task to twim {} during shutdown", rec);
        return;
    }

    rec.bgTaskFut = rec.bgTaskFut.then(std::forward<Func>(func));
}

TxnWIMetaManager::TxnWIMetaManager() {
    K2LOG_D(log::skvsvr, "ctor");
}

TxnWIMetaManager::~TxnWIMetaManager() {
    K2LOG_D(log::skvsvr, "dtor");
    _rwlist.clear();
    for (auto& [ts, twim] : _twims) {
        K2LOG_W(log::skvsvr, "Shutdown dropping twim: {} -> {}", ts, twim);
    }
}

TxnWIMeta* TxnWIMetaManager::getTxnWIMeta(dto::Timestamp ts) {
    if(auto it = _twims.find(ts); it != _twims.end()) {
        return &it->second;
    }
    return nullptr;
}

seastar::future<> TxnWIMetaManager::start(dto::Timestamp rts, std::shared_ptr<Persistence> persistence) {
    K2LOG_D(log::skvsvr, "starting");
    _cpo.init(_config.cpoEndpoint());
    _persistence = persistence;
    updateRetentionTimestamp(rts);

    _rwTimer.setCallback([this] {
        // swap the rwList for processing. This allows new twims to be added to the list for future processing
        // without changing what we're currently working over.
        return seastar::do_with(std::move(_rwlist), [this](auto& currentList) {
            K2LOG_D(log::skvsvr, "twim manager check rwe on {} twims", _rwlist.size());
            _rwlist.clear();  // we just moved it into currentList. clear it here just in case;
            // refresh the clock
            auto now = CachedSteadyClock::now(true);
            return seastar::do_until(
                [this, now, &currentList] {
                    return currentList.empty() || currentList.front().mtr.timestamp.compareCertain(_retentionTs) > 0;
                },
                [this, now, &currentList] {
                    if (currentList.empty()) {
                        return seastar::make_ready_future();
                    }
                    auto& twim = currentList.front();
                    currentList.pop_front();
                    K2LOG_W(log::skvsvr, "rw expired on: {}", twim);
                    auto status = _onAction(TxnWIMetaManager::Action::onRetentionWindowExpire, twim);
                    if (!status.is2xxOK()) {
                        K2LOG_E(log::skvsvr, "Failed processing twim {} RWE due to: {}", twim, status);
                    }
                    return seastar::make_ready_future();
                });
        });
    });
    _rwTimer.armPeriodic(_config.minimumRetentionPeriod());

    return seastar::make_ready_future();
}

seastar::future<> TxnWIMetaManager::gracefulStop() {
    K2LOG_I(log::skvsvr, "stopping twim mgr");
    _stopping = true;
    return _rwTimer.stop()
        .then([this] {
            K2LOG_I(log::skvsvr, "rw stopped. stopping with {} active twims", _twims.size());
            std::vector<seastar::future<>> bgFuts;
            for (auto& [_, twim]: _twims) {
                bgFuts.push_back(std::move(twim.bgTaskFut));
            }
            return seastar::when_all_succeed(bgFuts.begin(), bgFuts.end()).discard_result();
        });
}

void TxnWIMetaManager::updateRetentionTimestamp(dto::Timestamp rts) {
    K2LOG_D(log::skvsvr, "retention ts now={}", rts)
    _retentionTs = rts;
}

Status TxnWIMetaManager::addWrite(dto::K23SI_MTR&& mtr, dto::Key&& key, dto::Key&& trh, String&& trhCollection) {
    K2LOG_D(log::skvsvr, "Adding write for mtr={}, key={}, trh={}, trhCollection={}", mtr, key, trh, trhCollection);
    auto& rec = _twims[mtr.timestamp];
    if (rec.state == dto::TxnWIMetaState::Created) {
        rec.mtr = std::move(mtr);
        rec.trh = std::move(trh);
        rec.trhCollection = std::move(trhCollection);
        _rwlist.push_back(rec);
        rec.state = dto::TxnWIMetaState::Created;

        K2LOG_D(log::skvsvr, "created new twim record: {}", rec);
    }
    auto status = _onAction(Action::onCreate, rec);
    if (!status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to create new twim {} due to {}", rec, status);
        return status;
    }
    rec.writeKeys.insert(std::move(key));
    return Statuses::S200_OK("TWIM created");
}

Status TxnWIMetaManager::abortWrite(dto::Timestamp txnId, dto::Key key) {
    auto it = _twims.find(txnId);
    if (it == _twims.end()) {
        return Statuses::S404_Not_Found(fmt::format("transaction ID {} not found in abort for key {}", txnId, key));
    }
    it->second.writeKeys.erase(key);
    return _onAction(Action::onAbort, it->second);
}

Status TxnWIMetaManager::commitWrite(dto::Timestamp txnId, dto::Key key) {
    auto it = _twims.find(txnId);
    if (it == _twims.end()) {
        return Statuses::S404_Not_Found(fmt::format("transaction ID {} not found in commit for key {}", txnId, key));
    }
    it->second.writeKeys.erase(key);
    return _onAction(Action::onCommit, it->second);
}

Status TxnWIMetaManager::endTxn(dto::Timestamp txnId, dto::EndAction action) {
    auto it = _twims.find(txnId);
    if (it == _twims.end()) {
        return Statuses::S404_Not_Found(fmt::format("transaction ID {} not found in end {}", txnId, action));
    }
    return _onAction(action == dto::EndAction::Commit ? Action::onCommit : Action::onAbort, it->second);
}

Status TxnWIMetaManager::finalizingWIs(dto::Timestamp txnId) {
    auto it = _twims.find(txnId);
    if (it == _twims.end()) {
        return Statuses::S404_Not_Found(fmt::format("transaction ID {} not found in finalizing", txnId));
    }
    return _onAction(Action::onFinalize, it->second);
}

Status TxnWIMetaManager::finalizedTxn(dto::Timestamp txnId) {
    auto it = _twims.find(txnId);
    if (it == _twims.end()) {
        return Statuses::S404_Not_Found(fmt::format("transaction ID {} not found in finalized", txnId));
    }
    return _onAction(Action::onFinalized, it->second);
}

Status TxnWIMetaManager::_onAction(Action action, TxnWIMeta& twim) {
    K2LOG_D(log::skvsvr, "Processing action: ({})---({})--> in twim {}", twim.state, action, twim);
    switch (twim.state) {
        case dto::TxnWIMetaState::Created: switch (action) {
            case Action::onCreate: {
                return _inProgressPIP(twim);
            }
            case Action::onCommit:
            case Action::onAbort:
            case Action::onFinalize:
            case Action::onFinalized:
            case Action::onRetentionWindowExpire:
            case Action::onPersistSucceed:
            case Action::onPersistFail:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        case dto::TxnWIMetaState::InProgressPIP: switch (action) {
            case Action::onCreate: {
                return _inProgressPIP(twim);
            }
            case Action::onPersistSucceed: {
                return _inProgress(twim);
            }
            case Action::onPersistFail: {
                return _finalized(twim, false);
            }
            case Action::onCommit:
            case Action::onAbort:
            case Action::onFinalize:
            case Action::onFinalized:
            case Action::onRetentionWindowExpire:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        case dto::TxnWIMetaState::InProgress: switch (action) {
            case Action::onCreate: {
                return _inProgress(twim);
            }
            case Action::onCommit: {
                return _committed(twim);
            }
            case Action::onAbort: {
                return _aborted(twim);
            }
            case Action::onRetentionWindowExpire: {
                return _forceFinalize(twim);
            }
            case Action::onFinalize:
            case Action::onFinalized:
            case Action::onPersistSucceed:
            case Action::onPersistFail:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        case dto::TxnWIMetaState::Committed: switch (action) {
            case Action::onFinalize: // fall-through
            case Action::onRetentionWindowExpire: {
                return _finalizing(twim);
            }
            case Action::onCommit: {
                return _committed(twim);
            }
            case Action::onFinalized:
            case Action::onCreate:
            case Action::onAbort:
            case Action::onPersistSucceed:
            case Action::onPersistFail:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        case dto::TxnWIMetaState::Aborted: switch (action) {
            case Action::onFinalize: // fall-through
            case Action::onRetentionWindowExpire: {
                return _finalizing(twim);
            }
            case Action::onAbort: {
                return _aborted(twim);
            }
            case Action::onCreate:
            case Action::onCommit:
            case Action::onFinalized:
            case Action::onPersistSucceed:
            case Action::onPersistFail:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        case dto::TxnWIMetaState::ForceFinalize: switch (action) {
            case Action::onCommit: {
                return _committed(twim);
            }
            case Action::onAbort: {
                return _aborted(twim);
            }
            case Action::onCreate:
            case Action::onFinalize:
            case Action::onFinalized:
            case Action::onRetentionWindowExpire:
            case Action::onPersistSucceed:
            case Action::onPersistFail:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        case dto::TxnWIMetaState::FinalizingWIs: switch (action) {
            case Action::onFinalized: {
                return _finalizedPIP(twim);
            }
            case Action::onFinalize: {
                return _finalizing(twim);
            }
            case Action::onCreate:
            case Action::onCommit:
            case Action::onAbort:
            case Action::onRetentionWindowExpire:
            case Action::onPersistSucceed:
            case Action::onPersistFail:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        case dto::TxnWIMetaState::FinalizedPIP: switch (action) {
            case Action::onPersistSucceed: {
                return _finalized(twim, true);
            }
            case Action::onPersistFail: {
                K2LOG_E(log::skvsvr, "State transition: ({})---{}--->(DELETED) in twim {}", twim.state, action, twim);
                return _finalized(twim, false);
            }
            case Action::onCreate:
            case Action::onCommit:
            case Action::onAbort:
            case Action::onFinalize:
            case Action::onFinalized:
            case Action::onRetentionWindowExpire:
            default: {
                return Statuses::S500_Internal_Server_Error(fmt::format("Action {} not supported in state {}", action, twim.state));
            }
        }
        default: {
            return Statuses::S500_Internal_Server_Error(fmt::format("State {} not supported", twim.state));
        }
    }
}

Status TxnWIMetaManager::_inProgressPIP(TxnWIMeta& twim) {
    auto newState = dto::TxnWIMetaState::InProgressPIP;
    K2LOG_D(log::skvsvr, "Entering state: {}", newState);
    auto isRetry = twim.state == newState;
    twim.state = newState;
    if (!isRetry) {
        K2LOG_D(log::skvsvr, "State transition: {} for twim {}", newState, twim);
        auto fut = _persistence->append_cont(twim)
            .then([this, &twim] (auto&& status) {
                K2LOG_D(log::skvsvr, "persist completed for InProgressPIP of {} with {}", twim, status);
                if (!status.is2xxOK()) {
                    // flush didn't succeed
                    K2LOG_E(log::skvsvr, "persist failed for InProgressPIP of {} with {}", twim, status);
                    return seastar::make_ready_future<Status>(_onAction(Action::onPersistFail, twim));
                }
                return seastar::make_ready_future<Status>(_onAction(Action::onPersistSucceed, twim));
            });
        _addBgTask(twim, [fut = std::move(fut)]() mutable { return fut.discard_result(); });
    }
    // NB: We rely on Module::persistAfterFlush() to queue-up a retry
    return Statuses::S201_Created("created twim");
}

Status TxnWIMetaManager::_inProgress(TxnWIMeta& twim) {
    auto newState = dto::TxnWIMetaState::InProgress;
    K2LOG_D(log::skvsvr, "Entering state: {}", newState);
    twim.state = newState;
    return Statuses::S200_OK("processed state transition");
}

Status TxnWIMetaManager::_committed(TxnWIMeta& twim) {
    auto newState = dto::TxnWIMetaState::Committed;
    K2LOG_D(log::skvsvr, "Entering state: {}", newState);
    twim.state = newState;
    twim.finalizeAction = dto::EndAction::Commit;
    return Statuses::S200_OK("processed state transition");
}

Status TxnWIMetaManager::_aborted(TxnWIMeta& twim) {
    auto newState = dto::TxnWIMetaState::Aborted;
    K2LOG_D(log::skvsvr, "Entering state: {}", newState);
    twim.state = newState;
    twim.finalizeAction = dto::EndAction::Abort;
    return Statuses::S200_OK("processed state transition");
}

Status TxnWIMetaManager::_forceFinalize(TxnWIMeta& twim) {
    auto newState = dto::TxnWIMetaState::ForceFinalize;
    K2LOG_D(log::skvsvr, "Entering state: {}", newState);
    twim.state = newState;
    K2LOG_D(log::skvsvr, "Reaching out to TRH to finalize twim {}", twim);
    auto request= std::make_unique<dto::K23SIInspectTxnRequest>(
        dto::K23SIInspectTxnRequest{
            dto::PVID{}, // Will be filled in by PartitionRequest
            twim.trhCollection,
            twim.trh,
            twim.mtr.timestamp});

    auto fut = _cpo.partitionRequest<dto::K23SIInspectTxnRequest, dto::K23SIInspectTxnResponse, dto::Verbs::K23SI_INSPECT_TXN>(Deadline(_config.readTimeout()), *request)
        .then([this, &twim, request=std::move(request)] (auto&& result) {
            (void)request; // to be deleted by unique_ptr
            auto& [status, resp] = result;
            K2LOG_D(log::skvsvr, "inspect for twim {}, received response status={}, resp={}", twim, status, resp);
            // only trigger action if the TRH gives a response. Otherwise we have to retry
            // for all 3 cases below, we just rely on the RWE timer to trigger the next transition
            twim.unlinkRW(_rwlist);
            _rwlist.push_back(twim);
            if (status.is2xxOK()) {
                return seastar::make_ready_future<Status>(_onAction(resp.finalizeAction == dto::EndAction::Commit ? Action::onCommit : Action::onAbort, twim));
            }
            else if (status == dto::K23SIStatus::KeyNotFound) {
                return seastar::make_ready_future<Status>(_onAction(Action::onAbort, twim));
            }
            K2LOG_W(log::skvsvr, "Unable to communicate with TRH for {} after RWE", twim);
            return seastar::make_ready_future<Status>(status);
        });
    _addBgTask(twim, [fut=std::move(fut)]() mutable { return fut.discard_result(); });
    return Statuses::S200_OK("processed state transition");
}

Status TxnWIMetaManager::_finalizing(TxnWIMeta& twim) {
    auto newState = dto::TxnWIMetaState::FinalizingWIs;
    K2LOG_D(log::skvsvr, "Entering state: {}", newState);
    twim.state = newState;
    twim.unlinkRW(_rwlist);
    return Statuses::S200_OK("processed state transition");
}

Status TxnWIMetaManager::_finalizedPIP(TxnWIMeta& twim) {
    auto newState = dto::TxnWIMetaState::FinalizedPIP;
    K2LOG_D(log::skvsvr, "Entering state: {}", newState);
    twim.state = newState;

    auto fut = _persistence->append_cont(twim)
        .then([this, &twim] (auto&& status) {
            K2LOG_D(log::skvsvr, "persist completed for {} with {}", twim, status);
            if (!status.is2xxOK()) {
                // flush didn't succeed
                K2LOG_E(log::skvsvr, "persist failed for {} with {}", twim, status);
                return seastar::make_ready_future<Status>(_onAction(Action::onPersistFail, twim));
            }
            return seastar::make_ready_future<Status>(_onAction(Action::onPersistSucceed, twim));
        });
    _addBgTask(twim, [fut=std::move(fut)] () mutable { return fut.discard_result();});
    return Statuses::S201_Created("Finalized twim");
}

Status TxnWIMetaManager::_finalized(TxnWIMeta& twim, bool success) {
    // we don't have an actual state to model this - we just remove the record from tracking
    K2LOG_D(log::skvsvr, "Entering state: Finalized");
    _twims.erase(twim.mtr.timestamp);

    return success ? Statuses::S200_OK("twim processing completed") : Statuses::S500_Internal_Server_Error("Unable to persist txn metadata");
}

}  // namespace k2
