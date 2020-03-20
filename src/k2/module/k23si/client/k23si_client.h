//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->
#pragma once

#include <random>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/Collection.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

namespace k2 {

class K2TxnOptions{
public:
    K2TxnOptions() noexcept :
        deadline(Duration(1s)),
        priority(dto::TxnPriority::Medium) {}

    Deadline<> deadline;
    dto::TxnPriority priority;
};

template<typename ValueType>
class ReadResult {
public:
    ReadResult(Status s, dto::K23SIReadResponse<ValueType>&& r) : status(std::move(s)), response(std::move(r)) {}

    ValueType& getValue() {
        return response.value.val;
    }

    Status status;
private:
    dto::K23SIReadResponse<ValueType> response;
};

class WriteResult{
public:
    WriteResult(Status s, dto::K23SIWriteResponse&& r) : status(std::move(s)), response(std::move(r)) {}
    Status status;

private:
    dto::K23SIWriteResponse response;
};

class EndResult{
public:
    EndResult(Status s) : status(s) {}
    Status status;
};

class K2TxnHandle {
public:
    K2TxnHandle() = default;
    K2TxnHandle(dto::K23SI_MTR&& mtr, Deadline<> deadline, CPOClient* cpo, Duration d) noexcept :
        _mtr(std::move(mtr)), _deadline(deadline), _cpo_client(cpo), _started(true), 
         _failed(false), _failed_status(Status::S200_OK()), _txn_end_deadline(d)  {}

    template <typename ValueType>
    seastar::future<ReadResult<ValueType>> read(dto::Key key, const String& collection) {
        if (!_started) {
            return seastar::make_exception_future<ReadResult<ValueType>>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        if (_failed) {
            return seastar::make_ready_future<ReadResult<ValueType>>(ReadResult<ValueType>(_failed_status, dto::K23SIReadResponse<ValueType>()));
        }

        read_ops++;

        auto* request = new dto::K23SIReadRequest{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            collection,
            _mtr,
            std::move(key)
        };

        return _cpo_client->PartitionRequest
            <dto::K23SIReadRequest, dto::K23SIReadResponse<ValueType>, dto::Verbs::K23SI_READ>
            (_deadline, *request).
            then([this] (auto&& response) {
                auto& [status, k2response] = response;
                if (status == dto::K23SIStatus::AbortConflict() || 
                    status == dto::K23SIStatus::AbortRequestTooOld()) {
                    _failed = true;
                    _failed_status = status;
                }

                auto userResponse = ReadResult<ValueType>(std::move(status), std::move(k2response));
                return seastar::make_ready_future<ReadResult<ValueType>>(std::move(userResponse));
            }).finally([request] () { delete request; });
    }

    template <typename ValueType>
    seastar::future<WriteResult> write(dto::Key key, const String& collection, const ValueType& value) {
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

        auto* request = new dto::K23SIWriteRequest<ValueType>{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            collection,
            _mtr,
            _trh_key,
            false,
            _write_set.size() == 1,
            std::move(key),
            SerializeAsPayload<ValueType>{value}
        };

        return _cpo_client->PartitionRequest
            <dto::K23SIWriteRequest<ValueType>, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
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

    seastar::future<WriteResult> erase(dto::Key key, const String& collection);

    // Must be called exactly once by application code and after all ongoing read and write
    // operations are completed
    seastar::future<EndResult> end(bool shouldCommit);

    uint64_t read_ops{0};
    uint64_t write_ops{0};

private:
    dto::K23SI_MTR _mtr;
    Deadline<> _deadline{Deadline<>(10s)};
    CPOClient* _cpo_client;
    bool _started;
    bool _failed;
    Status _failed_status;
    Duration _txn_end_deadline;

    std::vector<dto::Key> _write_set;
    dto::Key _trh_key;
    std::string _trh_collection;
};

class K23SIClientConfig {
public:
    K23SIClientConfig(){};
};

class K23SIClient {
public:
    K23SIClient(const K23SIClientConfig &) {};
    K23SIClient(const K23SIClientConfig &, const std::vector<std::string>& _endpoints, std::string _cpo);

    seastar::future<Status> makeCollection(const String& collection);
    seastar::future<K2TxnHandle> beginTxn(const K2TxnOptions& options);

    ConfigDuration create_collection_deadline{"create_collection_deadline", 1s};
    ConfigDuration retention_window{"retention_window", 600s};
    ConfigDuration txn_end_deadline{"txn_end_deadline", 200ms};
    uint64_t read_ops{0};
    uint64_t write_ops{0};
private:
    std::mt19937 _gen;
    std::uniform_int_distribution<uint64_t> _rnd;
    std::vector<String> _k2endpoints;
    CPOClient _cpo_client;
};

} // namespace k2
