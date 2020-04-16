//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->

#include "k23si_client.h"


namespace k2 {

K2TxnHandle::K2TxnHandle(dto::K23SI_MTR&& mtr, Deadline<> deadline, CPOClient* cpo, K23SIClient* client, Duration d, TimePoint start_time) noexcept :
    _mtr(std::move(mtr)), _deadline(deadline), _cpo_client(cpo), _client(client), _started(true), 
    _failed(false), _failed_status(Statuses::S200_OK("default fail status")), _txn_end_deadline(d), _start_time(start_time)
{}


void K2TxnHandle::checkResponseStatus(Status& status) {
    if (status == dto::K23SIStatus::AbortConflict ||
        status == dto::K23SIStatus::AbortRequestTooOld ||
        status == dto::K23SIStatus::OperationNotAllowed) {
        _failed = true;
        _failed_status = status;
    }

    if (status == dto::K23SIStatus::AbortConflict) {
        _client->abort_conflicts++;
    }

    if (status == dto::K23SIStatus::AbortRequestTooOld) {
        _client->abort_too_old++;
        K2WARN("Abort: txn too old: " << _mtr);
    }
}

void K2TxnHandle::makeHeartbeatTimer() {
    _heartbeat_timer = std::make_unique<seastar::timer<>>([this] () {
        _client->heartbeats++;

        auto* request = new dto::K23SITxnHeartbeatRequest {
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            _trh_collection,
            _trh_key,
            _mtr
        };

        auto it = _cpo_client->collections.find(_trh_collection);
        if (it == _cpo_client->collections.end()) {
            K2WARN("Heartbeat timer failed to get trh collection!");
        }
        Duration deadline = it->second.collection.metadata.heartbeatDeadline / 2;

        _heartbeat_future = _cpo_client->PartitionRequest
        <dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse, dto::Verbs::K23SI_TXN_HEARTBEAT>
        (Deadline<>(deadline), *request).then([this] (auto&& response) {
            auto& [status, k2response] = response;
            checkResponseStatus(status);
            if (_failed) {
                _heartbeat_timer->cancel();
            }
        }).finally([request] () { delete request; });
    });

    auto it = _cpo_client->collections.find(_trh_collection);
    if (it == _cpo_client->collections.end()) {
        K2WARN("Heartbeat timer failed to get trh collection!");
    }

    Duration period = it->second.collection.metadata.heartbeatDeadline / 2;
    _heartbeat_timer->arm_periodic(period);
}

seastar::future<EndResult> K2TxnHandle::end(bool shouldCommit) {
    if (!_write_set.size()) {
        _client->successful_txns++;
        return seastar::make_ready_future<EndResult>(EndResult(Statuses::S200_OK("default end result")));
    }

    if (_heartbeat_timer) {
        _heartbeat_timer->cancel();
    }

    auto* request  = new dto::K23SITxnEndRequest {
        dto::Partition::PVID(), // Will be filled in by PartitionRequest
        _trh_collection,
        _trh_key,
        _mtr,
        shouldCommit && !_failed ? dto::EndAction::Commit : dto::EndAction::Abort,
        std::move(_write_set)
    };

    return _cpo_client->PartitionRequest
        <dto::K23SITxnEndRequest, dto::K23SITxnEndResponse, dto::Verbs::K23SI_TXN_END>
        (Deadline<>(_txn_end_deadline), *request).
        then([this] (auto&& response) {
            auto& [status, k2response] = response;
            if (status.is2xxOK() && !_failed) {
                _client->successful_txns++;
            } else if (!status.is2xxOK()){
                K2INFO("TxnEndRequest failed: " << status);
            }

            return _heartbeat_future.then([this, s=std::move(status)] () {
                // TODO get min transaction time from TSO client
                auto time_spent = Clock::now() - _start_time;
                if (time_spent < 10us) {
                    auto sleep = 10us - time_spent;
                    return seastar::sleep(sleep).then([s=std::move(s)] () {
                        return seastar::make_ready_future<EndResult>(EndResult(std::move(s)));
                    });
                }

                return seastar::make_ready_future<EndResult>(EndResult(std::move(s)));
            });
        }).finally([request] () { delete request; });
}

seastar::future<WriteResult> K2TxnHandle::erase(dto::Key key, const String& collection) {
    return write<int>(std::move(key), collection, 0, true);
}

K23SIClient::K23SIClient(k2::App& baseApp, const K23SIClientConfig &) : 
        _tsoClient(baseApp.getDist<k2::TSO_ClientLib>().local()), _gen(std::random_device()()) {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;
    _metric_groups.add_group("K23SI_client", {
        sm::make_counter("read_ops", read_ops, sm::description("Total K23SI Read operations"), labels),
        sm::make_counter("write_ops", write_ops, sm::description("Total K23SI Write/Delete operations"), labels),
        sm::make_counter("total_txns", total_txns, sm::description("Total K23SI transactions began"), labels),
        sm::make_counter("successful_txns", successful_txns, sm::description("Total K23SI transactions ended successfully (committed or user aborted)"), labels),
        sm::make_counter("abort_conflicts", abort_conflicts, sm::description("Total K23SI transactions aborted due to conflict"), labels),
        sm::make_counter("abort_too_old", abort_too_old, sm::description("Total K23SI transactions aborted due to retention window expiration"), labels),
        sm::make_counter("heartbeats", heartbeats, sm::description("Total K23SI transaction heartbeats sent"), labels),
    });
}

seastar::future<> K23SIClient::start() {
    for (auto it = _tcpRemotes().begin(); it != _tcpRemotes().end(); ++it) {
        _k2endpoints.push_back(String(*it));
    }
    K2INFO("_cpo: " << _cpo());
    _cpo_client = CPOClient(String(_cpo()));

    return seastar::make_ready_future<>();
}

seastar::future<> K23SIClient::stop() {
    return seastar::make_ready_future<>();
}

seastar::future<Status> K23SIClient::makeCollection(const String& collection) {
    std::vector<String> endpoints = _k2endpoints;

    dto::CollectionMetadata metadata{
        .name = collection,
        .hashScheme = dto::HashScheme::HashCRC32C,
        .storageDriver = dto::StorageDriver::K23SI,
        .capacity = {},
        .retentionPeriod = Duration(retention_window())
    };

    return _cpo_client.CreateAndWaitForCollection(Deadline<>(create_collection_deadline()), std::move(metadata), std::move(endpoints));
}

seastar::future<K2TxnHandle> K23SIClient::beginTxn(const K2TxnOptions& options) {
    auto start_time = Clock::now();
    return _tsoClient.GetTimestampFromTSO(start_time)
    .then([this, start_time, options] (auto&& timestamp) {
        dto::K23SI_MTR mtr{
            _rnd(_gen),
            std::move(timestamp),
            options.priority
        };

        total_txns++;
        return seastar::make_ready_future<K2TxnHandle>(K2TxnHandle(std::move(mtr), options.deadline, &_cpo_client, this, txn_end_deadline(), start_time));
    });
}

} // namespace k2
