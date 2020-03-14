//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
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

enum MockMessageVerbs : Verb {
    GET_DATA_URL = 102
};

class K2TxnOptions{
public:
    Duration timeout;
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
    K2TxnHandle(TXEndpoint endpoint) noexcept : _endpoint(endpoint), _started(true) {}
    K2TxnHandle() noexcept : _endpoint(), _started(false) {}
    K2TxnHandle(K2TxnHandle&& from) noexcept : _endpoint(std::move(from._endpoint)), _started(from._started) {}
    K2TxnHandle& operator=(K2TxnHandle&& from) = default;
    ~K2TxnHandle() noexcept {}

    template <typename ValueType>
    future<ReadResult<ValueType>> read(dto::Key key, const String& collection) {
        if (!_started) {
            return make_exception_future<ReadResult<ValueType>>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        dto::K23SIReadRequest request = {};
        request.key = std::move(key);
        request.collectionName = std::move(collection);

        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse<ValueType>>(dto::Verbs::K23SI_READ, std::move(request), _endpoint, 1s).
        then([] (auto&& response) {
            auto& [status, k2response] = response;
            auto userResponse = ReadResult<ValueType>(std::move(status), std::move(k2response));
            return make_ready_future<ReadResult<ValueType>>(std::move(userResponse));
        });
    }

    template <typename ValueType>
    future<WriteResult> write(dto::Key key, const String& collection, ValueType&& value) {
        if (!_started) {
            return make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        dto::K23SIWriteRequest<ValueType> request = {};
        request.key = std::move(key);
        request.collectionName = std::move(collection);
        request.value.val = std::move(value);

        return RPC().callRPC<dto::K23SIWriteRequest<ValueType>, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, std::move(request), _endpoint, 1s).
        then([] (auto&& response) {
            auto& [status, k2response] = response;
            return make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
        });
    }

    template <typename ValueType>
    future<WriteResult> write(dto::Key key, const String& collection, const ValueType& value) {
        if (!_started) {
            return make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        dto::K23SIWriteRequest<ValueType> request = {};
        request.key = std::move(key);
        request.collectionName = std::move(collection);
        request.value.val = value;

        return RPC().callRPC<dto::K23SIWriteRequest<ValueType>, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, std::move(request), _endpoint, 1s).
        then([] (auto&& response) {
            auto& [status, k2response] = response;
            return make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
        });
    }

    future<EndResult> end(bool shouldCommit) { (void) shouldCommit; return make_ready_future<EndResult>(EndResult(Status::S200_OK())); };
private:
    TXEndpoint _endpoint;
    bool _started;
};

class K23SIClientConfig {
public:
    K23SIClientConfig(){};
};

class K23SIClient {
public:
    K23SIClient(const K23SIClientConfig &) {};
    K23SIClient(const K23SIClientConfig &, TXEndpoint endpoint): _remote_endpoint(std::move(endpoint)){};
    TXEndpoint _remote_endpoint;

    future<K2TxnHandle> beginTxn(const K2TxnOptions& options) {
        (void) options;
        return make_ready_future<K2TxnHandle>(K2TxnHandle(_remote_endpoint));
    };
};

} // namespace k2
