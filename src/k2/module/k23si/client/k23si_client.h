//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->
#pragma once

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
    ReadResult(Status&& s, dto::K23SIReadResponse<ValueType>&& r) : status(std::move(s)), response(std::move(r)) {}

    ValueType& getValue() {
        return response.value.val;
    }

    Status status;
private:
    dto::K23SIReadResponse<ValueType> response;
};

class WriteResult{
public:
    WriteResult(Status&& s, dto::K23SIWriteResponse&& r) : status(std::move(s)), response(std::move(r)) {}
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
    K2TxnHandle(const K2TxnOptions& options, CPOClient* cpo) noexcept :
        _options(options), _cpo_client(cpo), _started(true), _ended(false)  {}

    template <typename ValueType>
    seastar::future<ReadResult<ValueType>> read(dto::Key key, const String& collection) {
        if (!_started) {
            return seastar::make_exception_future<ReadResult<ValueType>>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        read_ops++;

        auto* request = new dto::K23SIReadRequest{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            collection,
            dto::K23SI_MTR(),
            std::move(key)
        };

        return _cpo_client->PartitionRequest
            <dto::K23SIReadRequest, dto::K23SIReadResponse<ValueType>, dto::Verbs::K23SI_READ>
            (_options.deadline, *request).
            then([] (auto&& response) {
                auto& [status, k2response] = response;
                auto userResponse = ReadResult<ValueType>(std::move(status), std::move(k2response));
                return seastar::make_ready_future<ReadResult<ValueType>>(std::move(userResponse));
            }).finally([request] () { delete request; });
    }

    template <typename ValueType>
    seastar::future<WriteResult> write(dto::Key key, const String& collection, const ValueType& value) {
        if (!_started) {
            return seastar::make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        write_ops++;

        auto* request = new dto::K23SIWriteRequest<ValueType>{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            collection,
            dto::K23SI_MTR(),
            dto::Key(),
            false,
            false,
            std::move(key),
            SerializeAsPayload<ValueType>{value}};

        return _cpo_client->PartitionRequest
            <dto::K23SIWriteRequest<ValueType>, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
            (_options.deadline, *request).
            then([] (auto&& response) {
                auto& [status, k2response] = response;
                return seastar::make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
            }).finally([request] () { delete request; });
    }

    seastar::future<EndResult> end(bool shouldCommit);

    uint64_t read_ops{0};
    uint64_t write_ops{0};

private:
    K2TxnOptions _options;
    CPOClient* _cpo_client;
    bool _started;
    bool _ended;
    Status _end_status;
    dto::TxnPriority _retry_priority;
    std::vector<dto::Key> _write_set;
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

    std::vector<String> _k2endpoints;
    CPOClient _cpo_client;

    uint64_t read_ops{0};
    uint64_t write_ops{0};
};

} // namespace k2
