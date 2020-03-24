//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->

#include "k23si_client.h"

namespace k2 {

K2TxnHandle::K2TxnHandle(dto::K23SI_MTR&& mtr, Deadline<> deadline, CPOClient* cpo, K23SIClient* client, Duration d) noexcept :
    _mtr(std::move(mtr)), _deadline(deadline), _cpo_client(cpo), _client(client), _started(true), 
    _failed(false), _failed_status(Status::S200_OK()), _txn_end_deadline(d)
{}


void K2TxnHandle::checkResponseStatus(Status& status) {
    if (status == dto::K23SIStatus::AbortConflict() || 
        status == dto::K23SIStatus::AbortRequestTooOld() ||
        status == dto::K23SIStatus::OperationNotAllowed()) {
        _failed = true;
        _failed_status = status;
    }

    if (status == dto::K23SIStatus::AbortConflict()) {
        _client->abort_conflicts++;
    }

    if (status == dto::K23SIStatus::AbortRequestTooOld()) {
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
        return seastar::make_ready_future<EndResult>(EndResult(Status::S200_OK()));
    }

    _heartbeat_timer->cancel();

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
            }

            // TODO min transaction time
            return _heartbeat_future.then([s=std::move(status)] () {
                return seastar::make_ready_future<EndResult>(EndResult(std::move(s)));
            });
        }).finally([request] () { delete request; });
}

seastar::future<WriteResult> K2TxnHandle::erase(dto::Key key, const String& collection) {
    return write<int>(std::move(key), collection, 0, true);
}

K23SIClient::K23SIClient(const K23SIClientConfig &, const std::vector<std::string>& _endpoints, std::string _cpo) : _gen(std::random_device()()) {
    for (auto it = _endpoints.begin(); it != _endpoints.end(); ++it) {
        _k2endpoints.push_back(String(*it));
    }
    _cpo_client = CPOClient(String(_cpo));

    _metric_groups.clear();
    std::vector<sm::label_instance> labels;
    _metric_groups.add_group("K23SI_client", {
        sm::make_derive("read_ops", read_ops, sm::description("Total K23SI Read operations"), labels),
        sm::make_derive("write_ops", write_ops, sm::description("Total K23SI Write/Delete operations"), labels),
        sm::make_derive("total_txns", total_txns, sm::description("Total K23SI transactions began"), labels),
        sm::make_derive("successful_txns", successful_txns, sm::description("Total K23SI transactions ended successfully (committed or user aborted)"), labels),
        sm::make_derive("abort_conflicts", abort_conflicts, sm::description("Total K23SI transactions aborted due to conflict"), labels),
        sm::make_derive("abort_too_old", abort_too_old, sm::description("Total K23SI transactions aborted due to retention window expiration"), labels),
        sm::make_derive("heartbeats", heartbeats, sm::description("Total K23SI transaction heartbeats sent"), labels),
    });
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
    // TODO TSO integration
    dto::K23SI_MTR mtr{
        _rnd(_gen),
        dto::Timestamp(),
        options.priority
    };

    total_txns++;

    return seastar::make_ready_future<K2TxnHandle>(K2TxnHandle(std::move(mtr), options.deadline, &_cpo_client, this, txn_end_deadline()));
}

} // namespace k2
