//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/cpo/CPOClient.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/Collection.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

using namespace seastar;

namespace k2 {

std::ostream& operator<<(std::ostream& os, const dto::Key& key) {
    return os << key.partitionKey << key.rangeKey;
}

class K2TxnOptions{
public:
    K2TxnOptions() noexcept :
        deadline(Duration(1s)), 
        priority(dto::TxnPriority::Medium) {}

    Deadline<> deadline;
    //Timestamp timestamp;
    dto::TxnPriority priority;
    // auto-retry policy...
};

template<typename ValueType>
class ReadResult {
public:
    ReadResult(Status&& s, dto::K23SIReadResponse<ValueType>&& r) : status(std::move(s)), response(std::move(r)) {}

    ValueType& getValue() {
        return response.value.val;
    }

    uint64_t getAbortPriority() {
        return response.abortPriority;
    }

    Status status;
private:
    dto::K23SIReadResponse<ValueType> response;
};

class WriteResult{
public:
    WriteResult(Status&& s, dto::K23SIWriteResponse&& r) : status(std::move(s)), response(std::move(r)) {}
    Status status;

    uint64_t getAbortPriority() {
        return response.abortPriority;
    }

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
        _options(options), _cpo_client(cpo), _started(true)  {}

    template <typename ValueType>
    future<ReadResult<ValueType>> read(dto::Key key, const String& collection) {
        if (!_started) {
            return make_exception_future<ReadResult<ValueType>>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        dto::K23SIReadRequest request{};
        request.key = std::move(key);
        request.collectionName = collection;

        return do_with(std::move(request), [this] (dto::K23SIReadRequest& request) {
            return _cpo_client->PartitionRequest
                <dto::K23SIReadRequest, dto::K23SIReadResponse<ValueType>, dto::Verbs::K23SI_READ>
                (_options.deadline, request).
                then([] (auto&& response) {
                    auto& [status, k2response] = response;
                    auto userResponse = ReadResult<ValueType>(std::move(status), std::move(k2response));
                    return make_ready_future<ReadResult<ValueType>>(std::move(userResponse));
                });
        });
    }

    template <typename ValueType>
    future<WriteResult> write(dto::Key key, const String& collection, ValueType&& value) {
        if (!_started) {
            return make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        dto::K23SIWriteRequest<ValueType> request{};
        request.key = std::move(key);
        request.collectionName = collection;
        request.value.val = std::move(value);

        return do_with(std::move(request), [this] (dto::K23SIWriteRequest<ValueType>& request) {
            return _cpo_client->PartitionRequest
                <dto::K23SIWriteRequest<ValueType>, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
                (_options.deadline, request).
                then([] (auto&& response) {
                    auto& [status, k2response] = response;
                    return make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
                });
        });
    }

    template <typename ValueType>
    future<WriteResult> write(dto::Key key, const String& collection, const ValueType& value) {
        if (!_started) {
            return make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        dto::K23SIWriteRequest<ValueType> request{};
        request.key = std::move(key);
        request.collectionName = collection;
        request.value.val = value;

        return do_with(std::move(request), [this] (dto::K23SIWriteRequest<ValueType>& request) {
            return _cpo_client->PartitionRequest
                <dto::K23SIWriteRequest<ValueType>, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
                (_options.deadline, request).
                then([] (auto&& response) {
                    auto& [status, k2response] = response;
                    return make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
                });
        });
    }

    future<EndResult> end(bool shouldCommit) { (void) shouldCommit; return make_ready_future<EndResult>(EndResult(Status::S200_OK())); };
private:
    K2TxnOptions _options;
    CPOClient* _cpo_client;
    bool _started;
};

class K23SIClientConfig {
public:
    K23SIClientConfig(){};
};

class K23SIClient {
public:
    K23SIClient(const K23SIClientConfig &) {};
    K23SIClient(const K23SIClientConfig &, const std::vector<std::string>& _endpoints, std::string _cpo) {
        for (auto it = _endpoints.begin(); it != _endpoints.end(); ++it) {
            _k2endpoints.push_back(String(*it));
        }
        _cpo_client = CPOClient(String(_cpo));
    };
    std::vector<String> _k2endpoints;
    CPOClient _cpo_client;

    future<Status> makeCollection(const String& collection) {
        std::vector<String> endpoints = _k2endpoints;

        dto::CollectionMetadata metadata{
            .name = collection,
            .hashScheme = dto::HashScheme::HashCRC32C,
            .storageDriver = dto::StorageDriver::K23SI,
            .capacity = {},
            .retentionPeriod = Duration(600s)
        };
 
        return _cpo_client.CreateAndWaitForCollection(Deadline<>(5s), std::move(metadata), std::move(endpoints));
    }

    future<K2TxnHandle> beginTxn(const K2TxnOptions& options) {
        return make_ready_future<K2TxnHandle>(K2TxnHandle(options, &_cpo_client));
    };
};

} // namespace k2
