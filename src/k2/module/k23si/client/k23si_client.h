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

// Represents a new or in-progress query (aka read scan with predicate and projection)
class Query {
public:
    Query() = default;

    template <typename T>
    dto::K23SIFilterLeafNode makeFilterLiteralNode(T operand);
    dto::K23SIFilterLeafNode makeFilterFieldRefNode(const String& fieldName, dto::FieldType fieldType);
    dto::K23SIFilterOpNode makeFilterOpNode(dto::K23SIFilterOp, std::vector<dto::K23SIFilterLeafNode>&& leafChildren, std::vector<dto::K23SIFilterOpNode>&& opChildren);

    void setFilterTreeRoot(dto::K23SIFilterOpNode&& root);

    void addProjection(const String& fieldName);
    void addProjection(const std::vector<String>& fieldNames);

    int32_t limitLeft = -1; // Negative means no limit
    bool includeVersionMismatch = false;
    bool isDone(); // If false, more results may be available

    // The user must specify the inclusive start and exclusive end keys for the range scan, but the client 
    // still needs to encode these keys so we use SKVRecords. The SKVRecords will be created with an 
    // appropriate schema by the client createQuery function. The user is then expected to serialize the 
    // key fields into the SKVRecords, similar to a single key read request.
    //
    // They must be a fully specified prefix of the key fields. For example, if the key fields are defined 
    // as {ID, NAME, TIMESTAMP} then {ID = 1, TIMESTAMP = 10} is not a valid start or end scanRecord, but 
    // {ID = 1, NAME = J} is valid.
    dto::SKVRecord startScanRecord;
    dto::SKVRecord endScanRecord;

private:
    std::shared_ptr<dto::Schema> schema = nullptr;
    bool done = false;
    bool inprogress = false; // Used to prevent user from changing predicates after query has started
    dto::Key continuationToken;
    dto::K23SIQueryRequest request;

    friend class K2TxnHandle;
    friend class K23SIClient;
};

class QueryResult {
public:
    QueryResult(Status s, dto::K23SIQueryResponse&& r);

    Status status;
    std::vector<SKVRecord> records;
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
    seastar::future<Query> createQuery(const String& collectionName, const String& schemaName);

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

    dto::K23SIReadRequest* makeReadRequest(const dto::SKVRecord& record) const;
    dto::K23SIWriteRequest* makeWriteRequest(dto::SKVRecord& record, bool erase);

    template <class T>
    dto::K23SIReadRequest* makeReadRequest(const T& user_record) const {
        dto::SKVRecord record(user_record.collectionName, user_record.schema);
        user_record.__writeFields(record);

        return makeReadRequest(record);
    }
       
    dto::K23SIPartialUpdateRequest* makePartialUpdateRequest(dto::SKVRecord& record, std::vector<uint32_t> fieldsToUpdate) {
        dto::Key key = record.getKey();
        
        if (!_write_set.size()) {
            _trh_key = key;
            _trh_collection = record.collectionName;
        }
        _write_set.push_back(key);
            
        return new dto::K23SIPartialUpdateRequest{
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            record.collectionName,
            _mtr,
            _trh_key,
            _write_set.size() == 1,
            key,
            record.storage.share(),
            fieldsToUpdate
        };
    }


public:
    K2TxnHandle() = default;
    K2TxnHandle(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle& operator=(K2TxnHandle&& o) noexcept = default;
    K2TxnHandle(dto::K23SI_MTR&& mtr, K2TxnOptions options, CPOClient* cpo, K23SIClient* client, Duration d, TimePoint start_time) noexcept;

    template <class T>
    seastar::future<ReadResult<T>> read(T record) {
        if (!_valid) {
            return seastar::make_exception_future<ReadResult<T>>(std::runtime_error("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(_failed_status, T()));
        }

        _client->read_ops++;
        _ongoing_ops++;

        dto::K23SIReadRequest* request = makeReadRequest(record);

        return _cpo_client->PartitionRequest
            <dto::K23SIReadRequest, dto::K23SIReadResponse, dto::Verbs::K23SI_READ>
            (_options.deadline, *request).
            then([this, request_schema=record.schema, request] (auto&& response) {
                auto& [status, k2response] = response;
                checkResponseStatus(status);
                _ongoing_ops--;

                if constexpr (std::is_same<T, dto::SKVRecord>()) {
                    if (!status.is2xxOK()) {
                        return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(std::move(status), SKVRecord()));
                    }

                    return _client->getSchema(request->collectionName, request_schema->name, k2response.value.schemaVersion)
                    .then([s=std::move(status), storage=std::move(k2response.value), request] (auto&& response) mutable {
                        auto& [status, schema_ptr] = response;
                        K2EXPECT(status.is2xxOK(), true);

                        if (!status.is2xxOK()) {
                            return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(dto::K23SIStatus::OperationNotAllowed("Matching schema could not be found"), SKVRecord()));
                        }

                        SKVRecord skv_record(request->collectionName, schema_ptr);
                        skv_record.storage = std::move(storage);
                        return seastar::make_ready_future<ReadResult<T>>(ReadResult<T>(std::move(s), std::move(skv_record)));
                    });
                } else {
                    T userResponseRecord{};

                    if (status.is2xxOK()) {
                        SKVRecord skv_record(request->collectionName, request_schema);
                        skv_record.storage = std::move(k2response.value);
                        userResponseRecord.__readFields(skv_record);
                    }

                    return ReadResult<T>(std::move(status), std::move(userResponseRecord));
                }
            }).finally([request] () { delete request; });
    }

    template <class T>
    seastar::future<WriteResult> write(T& record, bool erase=false) {
        if (!_valid) {
            return seastar::make_exception_future<WriteResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<WriteResult>(WriteResult(_failed_status, dto::K23SIWriteResponse()));
        }
        _client->write_ops++;
        _ongoing_ops++;

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
                _ongoing_ops--;

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

    template <typename T1>
    seastar::future<PartialUpdateResult> partialUpdate(T1& record, std::vector<k2::String> fieldsName) {
        std::vector<uint32_t> fieldsToUpdate;
        bool find = false;
        for (std::size_t i = 0; i < fieldsName.size(); ++i) {
            find = false;
            for (std::size_t j = 0; j < record.schema->fields.size(); ++j) {
                if (fieldsName[i] == record.schema->fields[j].name) {
                    fieldsToUpdate.push_back(j);
                    find = true;
                    break;
                }
            }
            if (find == false) return seastar::make_ready_future<PartialUpdateResult>(
                    PartialUpdateResult(dto::K23SIStatus::BadParameter("error parameter: fieldsToUpdate")) );
        }

        return partialUpdate(record, fieldsToUpdate);
    }

    template <typename T1>
    seastar::future<PartialUpdateResult> partialUpdate(T1& record, std::vector<uint32_t> fieldsToUpdate) {
        if (!_valid) {
            return seastar::make_exception_future<PartialUpdateResult>(std::runtime_error("Invalid use of K2TxnHandle"));
        }
        if (_failed) {
            return seastar::make_ready_future<PartialUpdateResult>(PartialUpdateResult(_failed_status));
        }
        _client->write_ops++;
        _ongoing_ops++;

        dto::K23SIPartialUpdateRequest* request = nullptr;
        if constexpr (std::is_same<T1, dto::SKVRecord>()) {
            request = makePartialUpdateRequest(record, fieldsToUpdate);
        } else {
            SKVRecord skv_record(record.collectionName, record.schema);
            record.__updateFields(skv_record);
            request = makePartialUpdateRequest(skv_record, fieldsToUpdate);
        }
        if (request == nullptr) {
            return seastar::make_ready_future<PartialUpdateResult> (
                    PartialUpdateResult(dto::K23SIStatus::BadParameter("error makePartialUpdateRequest()")) );
        }
        
        return _cpo_client->PartitionRequest
            <dto::K23SIPartialUpdateRequest, dto::K23SIPartialUpdateResponse, dto::Verbs::K23SI_PARTIAL_UPDATE>
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
            }).finally([request] () { delete request; });
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
