/*
MIT License

Copyright(c) 2020 Futurewei Cloud

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
#include <k2/tso/client/tso_clientlib.h>
#include <k2/common/Timer.h>


namespace k2 {

class K2TxnOptions{
public:
    K2TxnOptions() noexcept :
        deadline(Duration(1s)),
        priority(dto::TxnPriority::Medium) {}

    Deadline<> deadline;
    dto::TxnPriority priority;
    bool syncFinalize = false;
};

template<typename ValueType>
class ReadResult {
public:
    ReadResult(Status s, ValueType&& v) : status(std::move(s)), value(std::move(v)) {}

    Status status;
    ValueType value;
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
    seastar::future<> gracefulStop();
    seastar::future<Status> makeCollection(const String& collection, std::vector<k2::String>&& rangeEnds=std::vector<k2::String>());
    seastar::future<K2TxnHandle> beginTxn(const K2TxnOptions& options);
    static constexpr int64_t ANY_VERSION = -1;
    seastar::future<Schema> getSchema(const String& collectionName, const String& schemaName, int64_t schemaVersion);

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

    CPOClient cpo_client;
private:
    sm::metric_groups _metric_groups;
    std::mt19937 _gen;
    std::uniform_int_distribution<uint64_t> _rnd;
    std::vector<String> _k2endpoints;
    // collection name -> (schema name -> (schema version -> schema))
    std::unordered_map<String, std::unordered_map<String, std::unordered_map<uint32_t, dto::Schema>>> _schemas;
};


class K2TxnHandle {
private:
    void makeHeartbeatTimer();
    void checkResponseStatus(Status& status);
public:
    K2TxnHandle() = default;
    K2TxnHandle(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle& operator=(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle(dto::K23SI_MTR&& mtr, K2TxnOptions options, CPOClient* cpo, K23SIClient* client, Duration d, TimePoint start_time) noexcept;

    dto::K23SIReadRequest* makeReadRequest(const dto::SKVRecord& record) const {
        return new dto::K23SIReadRequest{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            record.collectionName,
            _mtr,
            record.getKey()
        };
    }

    template <class T>
    dto::K23SIReadRequest* makeReadRequest(const T& user_record) const {
        dto::SKVRecord record(user_record.collectionName, user_record.schema);
        user_record.__writeFields(record);

        return makeReadRequest(record);
    }

    template <class T>
    seastar::future<ReadResult<T>> read(T record) {
        if (!_started) {
            return seastar::make_exception_future<ReadResult<T>>(std::runtime_error("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(_failed_status, T()));
        }

        _client->read_ops++;

        dto::K23SIReadRequest* request = makeReadRequest(record);

        return _cpo_client->PartitionRequest
            <dto::K23SIReadRequest, dto::K23SIReadResponse, dto::Verbs::K23SI_READ>
            (_options.deadline, *request).
            then([this, request_schema=record.schema, request_cname=record.collectionName] (auto&& response) {
                auto& [status, k2response] = response;
                checkResponseStatus(status);

                if constexpr (std::is_same<T, dto::SKVRecord>()) {
                    return ReadResult<SKVRecord>(std::move(status), std::move(k2response.value));
                } else {
                    T userResponseRecord{};

                    if (status.is2xxOK()) {
                        SKVRecord skv_record(request_cname, request_schema);
                        skv_record.storage = std::move(k2response.value);
                        userResponseRecord.__readFields(skv_record);
                    }

                    return ReadResult<T>(std::move(status), std::move(userResponseRecord));
                }
            }).finally([request] () { delete request; });
    }

    dto::K23SIWriteRequest* makeWriteRequest(dto::SKVRecord& record, bool erase) {
        dto::Key key = record.getKey();

        if (!_write_set.size()) {
            _trh_key = key;
            _trh_collection = record.collectionName;
        }
        _write_set.push_back(key);

        return new dto::K23SIWriteRequest{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            record.collectionName,
            _mtr,
            _trh_key,
            erase,
            _write_set.size() == 1,
            key,
            record.storage.share()
        };
    }

    template <class T>
    seastar::future<WriteResult> write(T& record, bool erase=false) {
        if (!_started) {
            return seastar::make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<WriteResult>(WriteResult(_failed_status, dto::K23SIWriteResponse()));
        }
        _client->write_ops++;

        dto::K23SIWriteRequest* request = nullptr;
        if constexpr (std::is_same<T, dto::SKVRecord>()) {
            request = makeWriteRequest(record, erase);
        } else {
            SKVRecord skv_record(record.collectionName, record.schema);
            record.__writeFields(skv_record);
            request = makeWriteRequest(skv_record, erase);
        }

        return _cpo_client->PartitionRequest
            <dto::K23SIWriteRequest, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
            (_options.deadline, *request).
            then([this] (auto&& response) {
                auto& [status, k2response] = response;
                checkResponseStatus(status);

                if (status.is2xxOK() && !_heartbeat_timer.isArmed()) {
                    K2ASSERT(_cpo_client->collections.find(_trh_collection) != _cpo_client->collections.end(), "collection not present after successful write");
                    K2DEBUG("Starting hb, mtr=" << _mtr << ", this=" << ((void*)this))
                    _heartbeat_interval = _cpo_client->collections[_trh_collection].collection.metadata.heartbeatDeadline / 2;
                    makeHeartbeatTimer();
                    _heartbeat_timer.armPeriodic(_heartbeat_interval);
                }

                return seastar::make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
            }).finally([request] () { delete request; });
    }

    seastar::future<WriteResult> erase(SKVRecord& record);

    // Must be called exactly once by application code and after all ongoing read and write
    // operations are completed
    seastar::future<EndResult> end(bool shouldCommit);
    friend std::ostream& operator<<(std::ostream& os, const K2TxnHandle& h){
        return os << h._mtr;
    }
private:
    dto::K23SI_MTR _mtr;
    K2TxnOptions _options;
    CPOClient* _cpo_client;
    K23SIClient* _client;
    bool _started;
    bool _failed;
    Status _failed_status;
    Duration _txn_end_deadline;
    TimePoint _start_time;

    Duration _heartbeat_interval;
    PeriodicTimer _heartbeat_timer;
    std::vector<dto::Key> _write_set;
    dto::Key _trh_key;
    String _trh_collection;
};

} // namespace k2
