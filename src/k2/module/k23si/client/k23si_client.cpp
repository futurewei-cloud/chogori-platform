//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->

#include "k23si_client.h"

namespace k2 {

seastar::future<EndResult> K2TxnHandle::end(bool shouldCommit) {
    if (!_write_set.size()) {
        return seastar::make_ready_future<EndResult>(EndResult(Status::S200_OK()));
    }

    // TODO min transaction time

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
        then([] (auto&& response) {
            auto& [status, k2response] = response;
            return seastar::make_ready_future<EndResult>(EndResult(std::move(status)));
        }).finally([request] () { delete request; });
}

seastar::future<WriteResult> K2TxnHandle::erase(dto::Key key, const String& collection) {
    if (!_started) {
        return seastar::make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
    }

    if (_failed) {
        return seastar::make_ready_future<WriteResult>(WriteResult(_failed_status, dto::K23SIWriteResponse()));
    }

    if (!_write_set.size()) {
        _trh_key = key;
        _trh_collection = collection;
    }
    _write_set.push_back(key);
    write_ops++;

    auto* request = new dto::K23SIWriteRequest<Payload>{
        dto::Partition::PVID(), // Will be filled in by PartitionRequest
        collection,
        _mtr,
        _trh_key,
        true,
        _write_set.size() == 1,
        std::move(key),
        Payload()
    };

    return _cpo_client->PartitionRequest
        <dto::K23SIWriteRequest<Payload>, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
        (_deadline, *request).
        then([this] (auto&& response) {
            auto& [status, k2response] = response;
            if (status == dto::K23SIStatus::AbortConflict() || 
                status == dto::K23SIStatus::AbortRequestTooOld()) {
                _failed = true;
                _failed_status = status;
            }

            return seastar::make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
        }).finally([request] () { delete request; });
}

K23SIClient::K23SIClient(const K23SIClientConfig &, const std::vector<std::string>& _endpoints, std::string _cpo) : _gen(std::random_device()()) {
    for (auto it = _endpoints.begin(); it != _endpoints.end(); ++it) {
        _k2endpoints.push_back(String(*it));
    }
    _cpo_client = CPOClient(String(_cpo));
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

    return seastar::make_ready_future<K2TxnHandle>(K2TxnHandle(std::move(mtr), options.deadline, &_cpo_client, txn_end_deadline()));
}

} // namespace k2
