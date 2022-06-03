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
#include <skvhttp/common/Common.h>
#include <skvhttp/common/Status.h>

#include <skvhttp/dto/Collection.h>
#include <skvhttp/dto/ControlPlaneOracle.h>
#include <skvhttp/dto/SKVRecord.h>
#include <skvhttp/dto/Timestamp.h>
#include <skvhttp/dto/Expression.h>

namespace skv::http::dto {

// common transaction priorities
enum class TxnPriority : uint8_t {
    Highest = 0,
    High = 64,
    Medium = 128,
    Low = 192,
    Lowest = 255
};

inline std::ostream& operator<<(std::ostream& os, const TxnPriority& pri) {
    const char* strpri = "bad priority";
    switch (pri) {
        case TxnPriority::Highest: strpri= "highest"; break;
        case TxnPriority::High: strpri= "high"; break;
        case TxnPriority::Medium: strpri= "medium"; break;
        case TxnPriority::Low: strpri= "low"; break;
        case TxnPriority::Lowest: strpri= "lowest"; break;
        default: break;
    }
    return os << strpri;
}

struct TxnOptions {
    Duration timeout{1s};
    TxnPriority priority{TxnPriority::Medium};
    bool syncFinalize{false};
    K2_SERIALIZABLE_FMT(TxnOptions, timeout, priority, syncFinalize);
};

struct BeginTxnRequest {
    TxnOptions options;
    K2_SERIALIZABLE_FMT(BeginTxnRequest, options);
};

struct BeginTxnResponse {
    Timestamp timestamp;
    K2_SERIALIZABLE_FMT(BeginTxnResponse, timestamp);
};

// The main READ DTO.
struct ReadRequest {
    String collectionName;  // the name of the collection
    // use the name "key" so that we can use common routing from CPO client
    SKVRecord key;  // the key to read
    K2_SERIALIZABLE_FMT(ReadRequest, collectionName, key);
};

// The response for READs
struct ReadResponse {
    SKVRecord record;
    K2_SERIALIZABLE_FMT(ReadResponse, record);
};

K2_DEF_ENUM(ExistencePrecondition,
    None,
    Exists,
    NotExists
);

struct WriteRequest {
    PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName; // the name of the collection
    // The TRH key is used to find the K2 node which owns a transaction. It should be set to the key of
    // the first write (the write for which designateTRH was set to true)
    // Note that this is not an unique identifier for a transaction record - transaction records are
    // uniquely identified by the tuple (mtr, trh)
    Key trh;
    String trhCollection; // the collection for the TRH
    bool isDelete = false; // is this a delete write?
    bool designateTRH = false; // if this is set, the server which receives the request will be designated the TRH
    // Whether the server should reject the write if a previous version exists (like a SQL insert),
    // or reject a write if a previous version does not exists (e.g. to know if a delete actually deleted
    // a record). In the future we want more expressive preconditions, but those will be on the fields of
    // a record whereas this is the only record-level precondition that makes sense so it is its own flag
    ExistencePrecondition precondition = ExistencePrecondition::None;
    // Generated on the client and stored on by the server so that
    uint64_t request_id;
    // use the name "key" so that we can use common routing from CPO client
    Key key; // the key for the write
    SKVRecord::Storage value; // the value of the write
    std::vector<uint32_t> fieldsForPartialUpdate; // if size() > 0 then this is a partial update

    WriteRequest() = default;
    WriteRequest(PVID _pvid, String cname, Key _trh, String _trhCollection, bool _isDelete,
                      bool _designateTRH, ExistencePrecondition _precondition, uint64_t id, Key _key, SKVRecord::Storage _value,
                      std::vector<uint32_t> _fields) :
        pvid(std::move(_pvid)),
        collectionName(std::move(cname)),
        trh(std::move(_trh)),
        trhCollection(std::move(_trhCollection)),
        isDelete(_isDelete),
        designateTRH(_designateTRH),
        precondition(_precondition),
        request_id(id),
        key(std::move(_key)),
        value(std::move(_value)),
        fieldsForPartialUpdate(std::move(_fields)) {}

    K2_SERIALIZABLE_FMT(WriteRequest, pvid, collectionName, trh, trhCollection, isDelete, designateTRH, precondition, request_id, key, value, fieldsForPartialUpdate);
};

struct WriteResponse {
    K2_SERIALIZABLE_FMT(WriteResponse);
};

struct QueryRequest {
    PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    // use the name "key" so that we can use common routing from CPO client
    Key key; // key for routing and will be interpreted as inclusive start key by the server
    Key endKey; // exclusive scan end key
    bool exclusiveKey = false; // Used to indicate key(aka startKey) is excluded in results

    int32_t recordLimit = -1; // Max number of records server should return, negative is no limit
    bool includeVersionMismatch = false; // Whether mismatched schema versions should be included in results
    bool reverseDirection = false; // If true, key should be high and endKey low

    expression::Expression filterExpression; // the filter expression for this query
    std::vector<String> projection; // Fields by name to include in projection

    K2_SERIALIZABLE_FMT(QueryRequest, pvid, collectionName, key, endKey, exclusiveKey, recordLimit,
        includeVersionMismatch, reverseDirection, filterExpression, projection);
};

struct QueryResponse {
    Key nextToScan; // For continuation token
    bool exclusiveToken = false; // whether nextToScan should be excluded or included
    std::vector<SKVRecord::Storage> results;
    K2_SERIALIZABLE_FMT(QueryResponse, nextToScan, exclusiveToken, results);
};

// Represents a new or in-progress query (aka read scan with predicate and projection)
class Query {
public:
    void setFilterExpression(expression::Expression&& root);
    void setReverseDirection(bool reverseDirection);
    void setIncludeVersionMismatch(bool includeVersionMismatch);
    void setLimit(int32_t limit);

    void addProjection(const String& fieldName);
    void addProjection(const std::vector<String>& fieldNames);

    bool isDone();  // If false, more results may be available

    // Recursively copies the payloads if the expression's values and children. This is used so that the
    // memory of the payloads will be allocated in the context of the current thread.
    void copyPayloads() { request.filterExpression.copyPayloads(); }

    // The user must specify the inclusive start and exclusive end keys for the range scan, but the client
    // still needs to encode these keys so we use SKVRecords. The SKVRecords will be created with an
    // appropriate schema by the client createQuery function. The user is then expected to serialize the
    // key fields into the SKVRecords, similar to a single key read request.
    //
    // They must be a fully specified prefix of the key fields. For example, if the key fields are defined
    // as {ID, NAME, TIMESTAMP} then {ID = 1, TIMESTAMP = 10} is not a valid start or end scanRecord, but
    // {ID = 1, NAME = J} is valid.
    SKVRecord startScanRecord;
    SKVRecord endScanRecord;
    K2_SERIALIZABLE_FMT(Query, startScanRecord, endScanRecord, done, inprogress, keysProjected, request);
    std::shared_ptr<Schema> schema = nullptr;

private:
    void checkKeysProjected();

    bool done = false;
    bool inprogress = false;  // Used to prevent user from changing predicates after query has started
    bool keysProjected = true;
    QueryRequest request;
};

struct CreateQueryRequest {
    String collectionName;
    String schemaName;
    K2_SERIALIZABLE_FMT(CreateQueryRequest, collectionName, schemaName);
};

struct CreateQueryResponse {
    Query query;    // the query if the response was OK
    K2_SERIALIZABLE_FMT(CreateQueryResponse, query);
};

struct TxnHeartbeatRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the transaction we want to heartbeat.
    // use the name "key" so that we can use common routing from CPO client
    Key key;

    K2_SERIALIZABLE_FMT(TxnHeartbeatRequest, pvid, collectionName, key);
};

struct TxnHeartbeatResponse {
    K2_SERIALIZABLE_FMT(TxnHeartbeatResponse);
};

// This is the end action to be taken on the transaction and its writes. Here are the current
// use cases for this:
// 1. Client library is ending a transaction(commit/abort)
// 2. TRH is finalizing the transaction at a participant
// 3. A PUSH response is signaling what action should be done to the WI which triggered the PUSH
K2_DEF_ENUM(EndAction,
    None,
    Abort,
    Commit);

// Response for PUSH operation
struct TxnPushResponse {
    // the mtr of the winning transaction
    EndAction incumbentFinalization = EndAction::None;
    bool allowChallengerRetry = false;

    K2_SERIALIZABLE_FMT(TxnPushResponse, incumbentFinalization, allowChallengerRetry);
};

struct TxnEndRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the transaction to end.
    // use the name "key" so that we can use common routing from CPO client
    Key key;
    // the end action (Abort|Commit)
    EndAction action;
    // the ranges to which this transaction wrote. TRH will finalize WIs with each range when we commit/abort
    std::unordered_map<String, std::unordered_set<KeyRangeVersion>> writeRanges;
    // flag to tell if the server should finalize synchronously.
    // this is useful in cases where the client knows that the data from the txn will be accessed a lot after
    // the commit, so it may choose to wait in order to get better performance.
    // This flag does not impact correctness, just performance for certain workloads
    bool syncFinalize=false;
    // The interval from end to Finalize for a transaction
    Duration timeToFinalize{0};

    K2_SERIALIZABLE_FMT(TxnEndRequest, pvid, collectionName, key, action, writeRanges, syncFinalize, timeToFinalize);
};

struct TxnEndResponse {
    K2_SERIALIZABLE_FMT(TxnEndResponse);
};

} // ns skv::http::dto
