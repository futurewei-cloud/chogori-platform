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

#include "query.h"

namespace k2 {

struct K23SIClientException : public std::exception {
    String what_str;
    K23SIClientException(String s) : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override{ return what_str.c_str();}
};

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

class PartialUpdateResult{
public:
    PartialUpdateResult(Status s) : status(std::move(s)) {}
    Status status;
};

class EndResult{
public:
    EndResult(Status s) : status(std::move(s)) {}
    Status status;
};

// This is the response to a getSchema request
struct GetSchemaResult {
    Status status;                        // the status of the response
    std::shared_ptr<dto::Schema> schema;  // the schema if the response was OK
};

// This is the response to a createSchema request
struct CreateSchemaResult {
    Status status;  // the status of the response
};

class K23SIClientConfig {
public:
    K23SIClientConfig(){};
};

struct CreateQueryResult {
    Status status;                        // the status of the response
    Query query;  // the query if the response was OK
};

class K2TxnHandle;

class K23SIClient {
public:
    K23SIClient(const K23SIClientConfig &);
private:
    TSO_ClientLib& _tsoClient;
public:

    seastar::future<> start();
    seastar::future<> gracefulStop();
    seastar::future<Status> makeCollection(const String& collection, std::vector<String>&& rangeEnds=std::vector<String>());
    seastar::future<K2TxnHandle> beginTxn(const K2TxnOptions& options);
    static constexpr int64_t ANY_VERSION = -1;
    seastar::future<GetSchemaResult> getSchema(const String& collectionName, const String& schemaName, int64_t schemaVersion);
    seastar::future<CreateSchemaResult> createSchema(const String& collectionName, dto::Schema schema);
    seastar::future<CreateQueryResult> createQuery(const String& collectionName, const String& schemaName);

    ConfigVar<std::vector<String>> _tcpRemotes{"tcp_remotes"};
    ConfigVar<String> _cpo{"cpo"};
    ConfigDuration create_collection_deadline{"create_collection_deadline", 1s};
    ConfigDuration retention_window{"retention_window", 600s};
    ConfigDuration txn_end_deadline{"txn_end_deadline", 60s};

    uint64_t read_ops{0};
    uint64_t write_ops{0};
    uint64_t query_ops{0};
    uint64_t total_txns{0};
    uint64_t successful_txns{0};
    uint64_t abort_conflicts{0};
    uint64_t abort_too_old{0};
    uint64_t heartbeats{0};

    CPOClient cpo_client;
    // collection name -> (schema name -> (schema version -> schemaPtr))
    std::unordered_map<String, std::unordered_map<String, std::unordered_map<uint32_t, std::shared_ptr<dto::Schema>>>> schemas;

private:
    seastar::future<Status> refreshSchemaCache(const String& collectionName);
    seastar::future<std::tuple<Status, std::shared_ptr<dto::Schema>>> getSchemaInternal(const String& collectionName, const String& schemaName, int64_t schemaVersion, bool doCPORefresh = true);

    sm::metric_groups _metric_groups;
    std::mt19937 _gen;
    std::uniform_int_distribution<uint64_t> _rnd;
    std::vector<String> _k2endpoints;
};


class K2TxnHandle {
private:
    void makeHeartbeatTimer();
    void checkResponseStatus(Status& status);

    std::unique_ptr<dto::K23SIReadRequest> makeReadRequest(const dto::SKVRecord& record) const;
    std::unique_ptr<dto::K23SIWriteRequest> makeWriteRequest(dto::SKVRecord& record, bool erase);

    template <class T>
    std::unique_ptr<dto::K23SIReadRequest> makeReadRequest(const T& user_record) const {
        dto::SKVRecord record(user_record.collectionName, user_record.schema);
        user_record.__writeFields(record);

        return makeReadRequest(record);
    }
       
    std::unique_ptr<dto::K23SIWriteRequest> makePartialUpdateRequest(dto::SKVRecord& record, 
            std::vector<uint32_t> fieldsForPartialUpdate);

    void prepareQueryRequest(Query& query);

public:
    K2TxnHandle() = default;
    K2TxnHandle(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle& operator=(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle(dto::K23SI_MTR&& mtr, K2TxnOptions options, CPOClient* cpo, K23SIClient* client, Duration d, TimePoint start_time) noexcept;

    template <class T>
    seastar::future<ReadResult<T>> read(T record) {
        if (!_valid) {
            return seastar::make_exception_future<ReadResult<T>>(K23SIClientException("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(_failed_status, T()));
        }

        std::unique_ptr<dto::K23SIReadRequest> request = makeReadRequest(record);

        _client->read_ops++;
        _ongoing_ops++;

        return _cpo_client->PartitionRequest
            <dto::K23SIReadRequest, dto::K23SIReadResponse, dto::Verbs::K23SI_READ>
            (_options.deadline, *request).
            then([this, request_schema=record.schema, &collName=request->collectionName] (auto&& response) {
                auto& [status, k2response] = response;
                checkResponseStatus(status);
                _ongoing_ops--;

                if constexpr (std::is_same<T, dto::SKVRecord>()) {
                    if (!status.is2xxOK()) {
                        return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(std::move(status), SKVRecord()));
                    }

                    return _client->getSchema(collName, request_schema->name, k2response.value.schemaVersion)
                    .then([s=std::move(status), storage=std::move(k2response.value), &collName] (auto&& response) mutable {
                        auto& [status, schema_ptr] = response;
                        K2EXPECT(status.is2xxOK(), true);

                        if (!status.is2xxOK()) {
                            return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(dto::K23SIStatus::OperationNotAllowed("Matching schema could not be found"), SKVRecord()));
                        }

                        SKVRecord skv_record(collName, schema_ptr);
                        skv_record.storage = std::move(storage);
                        return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(std::move(s), std::move(skv_record)));
                    });
                } else {
                    T userResponseRecord{};

                    if (status.is2xxOK()) {
                        SKVRecord skv_record(collName, request_schema);
                        skv_record.storage = std::move(k2response.value);
                        userResponseRecord.__readFields(skv_record);
                    }

                    return ReadResult<T>(std::move(status), std::move(userResponseRecord));
                }
            }).finally([r = std::move(request)] () { (void)r; });
    }

    template <class T>
    seastar::future<WriteResult> write(T& record, bool erase=false) {
        if (!_valid) {
            return seastar::make_exception_future<WriteResult>(K23SIClientException("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<WriteResult>(WriteResult(_failed_status, dto::K23SIWriteResponse()));
        }

        std::unique_ptr<dto::K23SIWriteRequest> request = nullptr;
        if constexpr (std::is_same<T, dto::SKVRecord>()) {
            request = makeWriteRequest(record, erase);
        } else {
            SKVRecord skv_record(record.collectionName, record.schema);
            record.__writeFields(skv_record);
            request = makeWriteRequest(skv_record, erase);
        }

        _client->write_ops++;
        _ongoing_ops++;

        return _cpo_client->PartitionRequest
            <dto::K23SIWriteRequest, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
            (_options.deadline, *request).
            then([this] (auto&& response) {
                auto& [status, k2response] = response;
                checkResponseStatus(status);
                _ongoing_ops--;

                if (status.is2xxOK() && !_heartbeat_timer.isArmed()) {
                    K2ASSERT(_cpo_client->collections.find(_trh_collection) != _cpo_client->collections.end(), "collection not present after successful write");
                    K2DEBUG("Starting hb, mtr=" << _mtr << ", this=" << ((void*)this))
                    _heartbeat_interval = _cpo_client->collections[_trh_collection].collection.metadata.heartbeatDeadline / 2;
                    makeHeartbeatTimer();
                    _heartbeat_timer.armPeriodic(_heartbeat_interval);
                }

                return seastar::make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
            }).finally([r = std::move(request)] () { (void) r; });
    }

    template <typename T1>
    seastar::future<PartialUpdateResult> partialUpdate(T1& record, std::vector<k2::String> fieldsName) {
        std::vector<uint32_t> fieldsForPartialUpdate;
        bool find = false;
        for (std::size_t i = 0; i < fieldsName.size(); ++i) {
            find = false;
            for (std::size_t j = 0; j < record.schema->fields.size(); ++j) {
                if (fieldsName[i] == record.schema->fields[j].name) {
                    fieldsForPartialUpdate.push_back(j);
                    find = true;
                    break;
                }
            }
            if (find == false) return seastar::make_ready_future<PartialUpdateResult>(
                    PartialUpdateResult(dto::K23SIStatus::BadParameter("error parameter: fieldsForPartialUpdate")) );
        }

        return partialUpdate(record, fieldsForPartialUpdate);
    }

    template <typename T1>
    seastar::future<PartialUpdateResult> partialUpdate(T1& record, std::vector<uint32_t> fieldsForPartialUpdate) {
        if (!_valid) {
            return seastar::make_exception_future<PartialUpdateResult>(K23SIClientException("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<PartialUpdateResult>(PartialUpdateResult(_failed_status));
        }
        _client->write_ops++;
        _ongoing_ops++;

        std::unique_ptr<dto::K23SIWriteRequest> request = nullptr;
        if constexpr (std::is_same<T1, dto::SKVRecord>()) {
            request = makePartialUpdateRequest(record, fieldsForPartialUpdate);
        } else {
            SKVRecord skv_record(record.collectionName, record.schema);
            record.__writeFields(skv_record);
            request = makePartialUpdateRequest(skv_record, fieldsForPartialUpdate);
        }
        if (request == nullptr) {
            return seastar::make_ready_future<PartialUpdateResult> (
                    PartialUpdateResult(dto::K23SIStatus::BadParameter("error makePartialUpdateRequest()")) );
        }
        
        return _cpo_client->PartitionRequest
            <dto::K23SIWriteRequest, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
            (_options.deadline, *request).
            then([this] (auto&& response) {
                auto& [status, k2response] = response;
                checkResponseStatus(status);
                _ongoing_ops--;
        
                if (status.is2xxOK() && !_heartbeat_timer.isArmed()) {
                    K2ASSERT(_cpo_client->collections.find(_trh_collection) != _cpo_client->collections.end(), "collection not present after successful partial update");
                    K2DEBUG("Starting hb, mtr=" << _mtr << ", this=" << ((void*)this))
                    _heartbeat_interval = _cpo_client->collections[_trh_collection].collection.metadata.heartbeatDeadline / 2;
                    makeHeartbeatTimer();
                    _heartbeat_timer.armPeriodic(_heartbeat_interval);
                }
        
                return seastar::make_ready_future<PartialUpdateResult>(PartialUpdateResult(std::move(status)));
            }).finally([r = std::move(request)] () { (void) r; });
    }
    
    seastar::future<WriteResult> erase(SKVRecord& record);

    // Get one set of paginated results for a query. User may need to call again with same query
    // object to get more results
    seastar::future<QueryResult> query(Query& query);

    // Must be called exactly once by application code and after all ongoing read and write
    // operations are completed
    seastar::future<EndResult> end(bool shouldCommit);

    // pretty print of the transaction handle
    friend std::ostream& operator<<(std::ostream& os, const K2TxnHandle& h){
        return os << h._mtr;
    }

    // use to obtain the MTR(which acts as a unique transaction identifier) for this transaction
    const dto::K23SI_MTR& mtr() const;

private:
    dto::K23SI_MTR _mtr;
    K2TxnOptions _options;
    CPOClient* _cpo_client = nullptr;
    K23SIClient* _client = nullptr;
    bool _valid = false; // If false, then was not create by beginTxn() or end() has already been called
    bool _failed = false;
    Status _failed_status;
    Duration _txn_end_deadline;
    TimePoint _start_time;
    uint64_t _ongoing_ops = 0; // Used to track if there are operations in flight when end() is called

    Duration _heartbeat_interval;
    PeriodicTimer _heartbeat_timer;
    std::vector<dto::Key> _write_set;
    dto::Key _trh_key;
    String _trh_collection;
};

} // namespace k2
