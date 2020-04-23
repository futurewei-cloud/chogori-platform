//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->
#pragma once

#include <random>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/Collection.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>
#include <k2/tso/client_lib/tso_clientlib.h>

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
    EndResult(Status s) : status(std::move(s)) {}
    Status status;
};

class K23SIClientConfig {
public:
    K23SIClientConfig(){};
};

class K2TxnHandle;

class K23SIClient {
public:
    K23SIClient(const K23SIClientConfig &);
private:
k2::TSO_ClientLib& _tsoClient;
public:

    seastar::future<> start();
    seastar::future<> stop();
    seastar::future<Status> makeCollection(const String& collection);
    seastar::future<K2TxnHandle> beginTxn(const K2TxnOptions& options);

    ConfigVar<std::vector<String>> _tcpRemotes{"tcp_remotes"};
    ConfigVar<String> _cpo{"cpo"};
    ConfigDuration create_collection_deadline{"create_collection_deadline", 1s};
    ConfigDuration retention_window{"retention_window", 600s};
    ConfigDuration txn_end_deadline{"txn_end_deadline", 60s};

    uint64_t read_ops{0};
    uint64_t write_ops{0};
    uint64_t total_txns{0};
    uint64_t successful_txns{0};
    uint64_t abort_conflicts{0};
    uint64_t abort_too_old{0};
    uint64_t heartbeats{0};
private:
    sm::metric_groups _metric_groups;
    std::mt19937 _gen;
    std::uniform_int_distribution<uint64_t> _rnd;
    std::vector<String> _k2endpoints;
    CPOClient _cpo_client;
};


class K2TxnHandle {
private:
    void makeHeartbeatTimer();
    void checkResponseStatus(Status& status);
public:
    K2TxnHandle() = default;
    K2TxnHandle(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle& operator=(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle(dto::K23SI_MTR&& mtr, Deadline<> deadline, CPOClient* cpo, K23SIClient* client, Duration d, TimePoint start_time) noexcept;

    template <typename ValueType>
    seastar::future<ReadResult<ValueType>> read(dto::Key key, const String& collection) {
        if (!_started) {
            return seastar::make_exception_future<ReadResult<ValueType>>(std::runtime_error("Invalid use of K2TxnHandle"));
        }

        if (_failed) {
            return seastar::make_ready_future<ReadResult<ValueType>>(ReadResult<ValueType>(_failed_status, dto::K23SIReadResponse<ValueType>()));
        }

        _client->read_ops++;

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
                checkResponseStatus(status);

                auto userResponse = ReadResult<ValueType>(std::move(status), std::move(k2response));
                return seastar::make_ready_future<ReadResult<ValueType>>(std::move(userResponse));
            }).finally([request] () { delete request; });
    }

    template <typename ValueType>
    seastar::future<WriteResult> write(dto::Key key, const String& collection, const ValueType& value, bool erase=false) {
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
        _client->write_ops++;

        auto* request = new dto::K23SIWriteRequest<ValueType>{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            collection,
            _mtr,
            _trh_key,
            erase,
            _write_set.size() == 1,
            std::move(key),
            SerializeAsPayload<ValueType>{value}
        };

        return _cpo_client->PartitionRequest
            <dto::K23SIWriteRequest<ValueType>, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
            (_deadline, *request).
            then([this] (auto&& response) {
                auto& [status, k2response] = response;
                checkResponseStatus(status);

                if (status.is2xxOK() && !_heartbeat_timer.armed()) {
                    K2ASSERT(_cpo_client->collections.find(_trh_collection) != _cpo_client->collections.end(), "collection not present after successful write");
                    K2DEBUG("Starting hb, mtr=" << _mtr << ", this=" << ((void*)this))
                    _heartbeat_interval = _cpo_client->collections[_trh_collection].collection.metadata.heartbeatDeadline / 2;
                    makeHeartbeatTimer();
                    _heartbeat_timer.arm_periodic(_heartbeat_interval);
                }

                return seastar::make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
            }).finally([request] () { delete request; });
    }

    seastar::future<WriteResult> erase(dto::Key key, const String& collection);

    // Must be called exactly once by application code and after all ongoing read and write
    // operations are completed
    seastar::future<EndResult> end(bool shouldCommit);
    friend std::ostream& operator<<(std::ostream& os, const K2TxnHandle& h){
        return os << h._mtr;
    }
private:
    dto::K23SI_MTR _mtr;
    Deadline<> _deadline{Deadline<>(10s)};
    CPOClient* _cpo_client;
    K23SIClient* _client;
    bool _started;
    bool _failed;
    Status _failed_status;
    Duration _txn_end_deadline;
    TimePoint _start_time;

    Duration _heartbeat_interval;
    seastar::timer<> _heartbeat_timer;
    seastar::future<> _heartbeat_future{seastar::make_ready_future()};
    std::vector<dto::Key> _write_set;
    dto::Key _trh_key;
    String _trh_collection;
};

} // namespace k2
