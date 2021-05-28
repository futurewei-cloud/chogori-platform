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

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/common/Log.h>
#include <k2/common/Timer.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>
#include <k2/tso/client/tso_clientlib.h>

#include <random>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

#include "Log.h"
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
    K2_DEF_FMT(K2TxnOptions, deadline, priority, syncFinalize);
};

template<typename ValueType>
class ReadResult {
public:
    ReadResult(Status s, ValueType&& v) : status(std::move(s)), value(std::move(v)) {}

    Status status;
    ValueType value;
    K2_DEF_FMT(ReadResult, status);
};

class WriteResult{
public:
    WriteResult(Status s, dto::K23SIWriteResponse&& r) : status(std::move(s)), response(std::move(r)) {}
    Status status;
    K2_DEF_FMT(WriteResult, status);

private:
    dto::K23SIWriteResponse response;
};

class PartialUpdateResult{
public:
    PartialUpdateResult(Status s) : status(std::move(s)) {}
    Status status;
    K2_DEF_FMT(PartialUpdateResult, status);
};

class EndResult{
public:
    EndResult(Status s) : status(std::move(s)) {}
    Status status;
    K2_DEF_FMT(EndResult, status);
};

// This is the response to a getSchema request
struct GetSchemaResult {
    Status status;                        // the status of the response
    std::shared_ptr<dto::Schema> schema;  // the schema if the response was OK
    K2_DEF_FMT(GetSchemaResult, status);
};

// This is the response to a createSchema request
struct CreateSchemaResult {
    Status status;  // the status of the response
    K2_DEF_FMT(CreateSchemaResult, status);
};

class K23SIClientConfig {
public:
    K23SIClientConfig(){};
};

struct CreateQueryResult {
    Status status;                        // the status of the response
    Query query;  // the query if the response was OK
    K2_DEF_FMT(CreateQueryResult, status, query);
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
    // First version of makeCollection is deprecated, do not use.
    // TODO: remove first version of makeCollection and update the test code.
    seastar::future<Status> makeCollection(const String& collection, std::vector<String>&& rangeEnds=std::vector<String>());
    seastar::future<Status> makeCollection(dto::CollectionMetadata&& metadata, std::vector<String>&& endpoints, std::vector<String>&& rangeEnds=std::vector<String>());
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
    std::vector<String> _k2endpoints;
};


class K2TxnHandle {
private:
    void _makeHeartbeatTimer();
    void _checkResponseStatus(Status& status);

    std::unique_ptr<dto::K23SIReadRequest> _makeReadRequest(const dto::Key& key,
                                                           const String& collectionName) const;
    std::unique_ptr<dto::K23SIWriteRequest> _makeWriteRequest(dto::SKVRecord& record, bool erase,
                                                             bool rejectIfExists);

    template <class T>
    std::unique_ptr<dto::K23SIReadRequest> _makeReadRequest(const T& user_record) const {
        dto::SKVRecord record(user_record.collectionName, user_record.schema);
        user_record.__writeFields(record);

        return _makeReadRequest(record.getKey(), record.collectionName);
    }

    std::unique_ptr<dto::K23SIWriteRequest> _makePartialUpdateRequest(dto::SKVRecord& record,
            std::vector<uint32_t> fieldsForPartialUpdate, dto::Key&& key);

    void _prepareQueryRequest(Query& query);

    // Utility method used to register the range for a given write request, after we receive a response for it.
    // We track these ranges so that we can tell the TRH to finalize WIs in them when the transaction ends.
    template <class T>
    void _registerRangeForWrite(Status& status, T& request) {
        // we only want to register a range for finalization in the happy case, and most error cases (e.g. Timeout)
        // in particular, we don't want to register a range if the error is one of the following:
        if (status != dto::K23SIStatus::AbortConflict && // there was a conflict and this write was told to abort
            status != dto::K23SIStatus::AbortRequestTooOld && // this write was rejected because it was too old
            status != dto::K23SIStatus::BadParameter && // the write was rejected due to a bad request parameter
            status != dto::K23SIStatus::ConditionFailed) { // the write was rejected since it specified rejectIfExists and there was an existing record
            // we're handling this after successfully finding a collection's partition and receiving some
            // response from it. The cpo client's collections map must have this collection
            if (auto it=_cpo_client->collections.find(request.collectionName); it != _cpo_client->collections.end()) {
                auto& krv = it->second->getPartitionForKey(request.key).partition->keyRangeV;
                _write_ranges[request.collectionName].insert(krv);
            }
            else {
                K2LOG_W(log::skvclient, "collection {} does not exist after handling with status {}", request.collectionName, status);
            }
        }
        else {
            K2LOG_D(log::skvclient, "Not registering write for key {} after response with status {}", request.key, status);
        }
    }

public:
    K2TxnHandle() = default;
    K2TxnHandle(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle& operator=(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle(dto::K23SI_MTR&& mtr, K2TxnOptions options, CPOClient* cpo, K23SIClient* client, Duration d, TimePoint start_time) noexcept;

    // The dto::Key oriented interface for read. The key should be one obtained from SKVRecord::getKey()
    // and not directly created by the user
    seastar::future<ReadResult<dto::SKVRecord>> read(dto::Key key, String collection);

    // The read interface for user-defined classes with the SKV_RECORD_FIELDS macro defined
    // The class instance is automatically serialized and deserialized from an SKVRecord
    // Note that there is an explicit template instantiation of this function for the normal SKVRecord
    // read interface
    template <class T>
    seastar::future<ReadResult<T>> read(T record) {
        if (!_valid) {
            return seastar::make_exception_future<ReadResult<T>>(K23SIClientException("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(_failed_status, T()));
        }

        std::unique_ptr<dto::K23SIReadRequest> request = _makeReadRequest(record);

        _client->read_ops++;
        _ongoing_ops++;

        return _cpo_client->partitionRequest
            <dto::K23SIReadRequest, dto::K23SIReadResponse, dto::Verbs::K23SI_READ>
            (_options.deadline, *request).
            then([this, request_schema=record.schema, &collName=request->collectionName] (auto&& response) {
                auto& [status, k2response] = response;
                _checkResponseStatus(status);
                _ongoing_ops--;

                T userResponseRecord{};

                if (status.is2xxOK()) {
                    SKVRecord skv_record(collName, request_schema, std::move(k2response.value), true);
                    userResponseRecord.__readFields(skv_record);
                }

                return ReadResult<T>(std::move(status), std::move(userResponseRecord));
            }).finally([r = std::move(request)] () { (void)r; });
    }

    template <class T>
    seastar::future<WriteResult> write(T& record, bool erase=false, bool rejectIfExists=false) {
        if (!_valid) {
            return seastar::make_exception_future<WriteResult>(K23SIClientException("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<WriteResult>(WriteResult(_failed_status, dto::K23SIWriteResponse()));
        }

        std::unique_ptr<dto::K23SIWriteRequest> request;
        if constexpr (std::is_same<T, dto::SKVRecord>()) {
            request = _makeWriteRequest(record, erase, rejectIfExists);
        } else {
            SKVRecord skv_record(record.collectionName, record.schema);
            record.__writeFields(skv_record);
            request = _makeWriteRequest(skv_record, erase, rejectIfExists);
        }

        _client->write_ops++;
        _ongoing_ops++;

        return _cpo_client->partitionRequest
            <dto::K23SIWriteRequest, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
            (_options.deadline, *request).
            then([this, request=std::move(request)] (auto&& response) {
                auto& [status, k2response] = response;

                _registerRangeForWrite(status, *request);

                _checkResponseStatus(status);
                _ongoing_ops--;

                if (status.is2xxOK() && !_heartbeat_timer.isArmed()) {
                    K2ASSERT(log::skvclient, _cpo_client->collections.find(_trh_collection) != _cpo_client->collections.end(), "collection not present after successful write");
                    K2LOG_D(log::skvclient, "Starting hb, mtr={}", _mtr);
                    _heartbeat_interval = _cpo_client->collections[_trh_collection]->collection.metadata.heartbeatDeadline / 2;
                    _makeHeartbeatTimer();
                    _heartbeat_timer.armPeriodic(_heartbeat_interval);
                }

                return seastar::make_ready_future<WriteResult>(WriteResult(std::move(status), std::move(k2response)));
            });
    }

    template <typename T>
    seastar::future<PartialUpdateResult> partialUpdate(T& record, std::vector<k2::String> fieldsName,
                                                       dto::Key key=dto::Key()) {
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

        return partialUpdate(record, std::move(fieldsForPartialUpdate), std::move(key));
    }

    template <typename T>
    seastar::future<PartialUpdateResult> partialUpdate(T& record,
                                                       std::vector<uint32_t> fieldsForPartialUpdate,
                                                       dto::Key key=dto::Key()) {
        if (!_valid) {
            return seastar::make_exception_future<PartialUpdateResult>(K23SIClientException("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<PartialUpdateResult>(PartialUpdateResult(_failed_status));
        }
        _client->write_ops++;
        _ongoing_ops++;

        std::unique_ptr<dto::K23SIWriteRequest> request;
        if constexpr (std::is_same<T, dto::SKVRecord>()) {
            if (key.partitionKey == "") {
                key = record.getKey();
            }

            request = _makePartialUpdateRequest(record, fieldsForPartialUpdate, std::move(key));
        } else {
            SKVRecord skv_record(record.collectionName, record.schema);
            record.__writeFields(skv_record);
            if (key.partitionKey == "") {
                key = skv_record.getKey();
            }

            request = _makePartialUpdateRequest(skv_record, fieldsForPartialUpdate, std::move(key));
        }
        if (!request) {
            return seastar::make_ready_future<PartialUpdateResult> (
                    PartialUpdateResult(dto::K23SIStatus::BadParameter("error _makePartialUpdateRequest()")) );
        }

        return _cpo_client->partitionRequest
            <dto::K23SIWriteRequest, dto::K23SIWriteResponse, dto::Verbs::K23SI_WRITE>
            (_options.deadline, *request).
            then([this, request=std::move(request)] (auto&& response) {
                auto& [status, k2response] = response;

                _registerRangeForWrite(status, *request);

                _checkResponseStatus(status);
                _ongoing_ops--;

                if (status.is2xxOK() && !_heartbeat_timer.isArmed()) {
                    K2ASSERT(log::skvclient, _cpo_client->collections.find(_trh_collection) != _cpo_client->collections.end(), "collection not present after successful partial update");
                    K2LOG_D(log::skvclient, "Starting hb, mtr={}", _mtr)
                    _heartbeat_interval = _cpo_client->collections[_trh_collection]->collection.metadata.heartbeatDeadline / 2;
                    _makeHeartbeatTimer();
                    _heartbeat_timer.armPeriodic(_heartbeat_interval);
                }

                return seastar::make_ready_future<PartialUpdateResult>(PartialUpdateResult(std::move(status)));
            });
    }

    seastar::future<WriteResult> erase(SKVRecord& record);

    // Get one set of paginated results for a query. User may need to call again with same query
    // object to get more results
    seastar::future<QueryResult> query(Query& query);

    // Must be called exactly once by application code and after all ongoing read and write
    // operations are completed
    seastar::future<EndResult> end(bool shouldCommit);

    // use to obtain the MTR(which acts as a unique transaction identifier) for this transaction
    const dto::K23SI_MTR& mtr() const;

    K2_DEF_FMT(K2TxnHandle, _mtr);

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

    // affected write ranges per collection
    std::unordered_map<String, std::unordered_set<dto::KeyRangeVersion>> _write_ranges;

    // the trh key and home collection for this transaction
    std::optional<dto::Key> _trh_key;
    String _trh_collection;
};

// Normal use-case read interface, where the key fields of the user's SKVRecord are
// serialized and converted into a dto::Key
template <>
seastar::future<ReadResult<dto::SKVRecord>> K2TxnHandle::read(dto::SKVRecord record);

} // namespace k2
