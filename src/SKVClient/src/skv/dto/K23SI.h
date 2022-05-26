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
#include <skv/common/Common.h>
#include <skv/common/Status.h>

#include "Collection.h"
#include "ControlPlaneOracle.h"
#include "SKVRecord.h"
#include "Timestamp.h"
#include "Expression.h"

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

// Minimum Transaction Record - enough to identify a transaction.
struct K23SI_MTR {
    Timestamp timestamp; // the TSO timestamp of the transaction
    TxnPriority priority = TxnPriority::Medium;  // transaction priority: user-defined: used to pick abort victims by K2 (0 is highest)
    bool operator==(const K23SI_MTR& o) const;
    bool operator!=(const K23SI_MTR& o) const;
    size_t hash() const;
    K2_PAYLOAD_FIELDS(timestamp, priority);
    K2_DEF_FMT(K23SI_MTR, timestamp, priority);
};
struct TxnOptions {
    Duration timeout{1s};
    TxnPriority priority{TxnPriority::Medium};
    bool syncFinalize{false};
    K2_PAYLOAD_FIELDS(timeout, priority, syncFinalize);
    K2_DEF_FMT(TxnOptions, timeout, priority, syncFinalize);
};

struct K23SIBeginTxnRequest {
    TxnOptions options;
    K2_PAYLOAD_FIELDS(options);
    K2_DEF_FMT(K23SIBeginTxnRequest, options);
};

struct K23SIBeginTxnResponse {
    Timestamp timestamp; // the TSO timestamp of the transaction
    K2_PAYLOAD_FIELDS(timestamp);
    K2_DEF_FMT(K23SIBeginTxnResponse, timestamp);
};

// The main READ DTO.
struct K23SIReadRequest {
    Timestamp timestamp; // the TSO timestamp of the transaction
    String collectionName;  // the name of the collection
    String schemaName; // the name of the schema
    uint32_t schemaVersion;
    // use the name "key" so that we can use common routing from CPO client
    SKVRecord::Storage key;  // the key to read
    K2_PAYLOAD_FIELDS(timestamp, collectionName, schemaName, schemaVersion, key);
    K2_DEF_FMT(K23SIReadRequest, timestamp, collectionName, schemaName, schemaVersion, key);
};

// The response for READs
struct K23SIReadResponse {
    SKVRecord::Storage storage;
    K2_PAYLOAD_FIELDS(storage);
    K2_DEF_FMT(K23SIReadResponse, storage);
};

K2_DEF_ENUM(ExistencePrecondition,
    None,
    Exists,
    NotExists
);

struct K23SIWriteRequest {
    Timestamp timestamp; // the TSO timestamp of the transaction
    String collectionName;  // the name of the collection
    String schemaName; // the name of the schema
    uint32_t schemaVersion;
    bool isDelete = false; // is this a delete write?
    // Whether the server should reject the write if a previous version exists (like a SQL insert),
    // or reject a write if a previous version does not exists (e.g. to know if a delete actually deleted
    // a record). In the future we want more expressive preconditions, but those will be on the fields of
    // a record whereas this is the only record-level precondition that makes sense so it is its own flag
    ExistencePrecondition precondition = ExistencePrecondition::None;
    SKVRecord::Storage value; // the value of the write
    K2_PAYLOAD_FIELDS(timestamp, collectionName, schemaName, schemaVersion, isDelete, value);
    K2_DEF_FMT(K23SIWriteRequest,timestamp, collectionName, schemaName, schemaVersion, isDelete, value);
};

struct K23SIWriteResponse {
    K2_PAYLOAD_FIELDS();
    K2_DEF_FMT(K23SIWriteResponse);
};

struct K23SIQueryRequest {
    PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    K23SI_MTR mtr; // the MTR for the issuing transaction
    // use the name "key" so that we can use common routing from CPO client
    Key key; // key for routing and will be interpreted as inclusive start key by the server
    Key endKey; // exclusive scan end key
    bool exclusiveKey = false; // Used to indicate key(aka startKey) is excluded in results

    int32_t recordLimit = -1; // Max number of records server should return, negative is no limit
    bool includeVersionMismatch = false; // Whether mismatched schema versions should be included in results
    bool reverseDirection = false; // If true, key should be high and endKey low

    expression::Expression filterExpression; // the filter expression for this query
    std::vector<String> projection; // Fields by name to include in projection

    K2_PAYLOAD_FIELDS(pvid, collectionName, mtr, key, endKey, exclusiveKey, recordLimit, includeVersionMismatch,
                      reverseDirection, filterExpression, projection);
    K2_DEF_FMT(K23SIQueryRequest, pvid, collectionName, mtr, key, endKey, exclusiveKey, recordLimit,
        includeVersionMismatch, reverseDirection, filterExpression, projection);
};

struct K23SIQueryResponse {
    Key nextToScan; // For continuation token
    bool exclusiveToken = false; // whether nextToScan should be excluded or included
    std::vector<SKVRecord::Storage> results;
    K2_PAYLOAD_FIELDS(nextToScan, exclusiveToken, results);
    K2_DEF_FMT(K23SIQueryResponse, nextToScan, exclusiveToken, results);
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
    K2_DEF_FMT(Query, startScanRecord, endScanRecord, done, inprogress, keysProjected, request);
    std::shared_ptr<Schema> schema = nullptr;
    K2_PAYLOAD_FIELDS(startScanRecord, endScanRecord, done, inprogress, keysProjected, request);

private:
    void checkKeysProjected();

    bool done = false;
    bool inprogress = false;  // Used to prevent user from changing predicates after query has started
    bool keysProjected = true;
    K23SIQueryRequest request;
};

struct K23SICreateQueryRequest {
    String collectionName;
    String schemaName;
    K2_PAYLOAD_FIELDS(collectionName, schemaName);
    K2_DEF_FMT(K23SICreateQueryRequest, collectionName, schemaName);
};

struct K23SICreateQueryResponse {
    Query query;    // the query if the response was OK
    K2_PAYLOAD_FIELDS(query);
    K2_DEF_FMT(K23SICreateQueryResponse, query);
};

struct K23SITxnHeartbeatRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the transaction we want to heartbeat.
    // use the name "key" so that we can use common routing from CPO client
    Key key;
    // the MTR for the transaction we want to heartbeat
    K23SI_MTR mtr;

    K2_PAYLOAD_FIELDS(pvid, collectionName, key, mtr);
    K2_DEF_FMT(K23SITxnHeartbeatRequest, pvid, collectionName, key, mtr);
};

struct K23SITxnHeartbeatResponse {
    K2_PAYLOAD_FIELDS();
    K2_DEF_FMT(K23SITxnHeartbeatResponse);
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
struct K23SITxnPushResponse {
    // the mtr of the winning transaction
    EndAction incumbentFinalization = EndAction::None;
    bool allowChallengerRetry = false;

    K2_PAYLOAD_FIELDS(incumbentFinalization, allowChallengerRetry);
    K2_DEF_FMT(K23SITxnPushResponse, incumbentFinalization, allowChallengerRetry);
};

struct K23SITxnEndRequest {
    // the TSO timestamp of the transaction
    Timestamp timestamp;
    // the end action (Abort|Commit)
    EndAction action{EndAction::Abort};
    K2_PAYLOAD_FIELDS(timestamp, action);
    K2_DEF_FMT(K23SITxnEndRequest, timestamp, action);
};

struct K23SITxnEndResponse {
    K2_PAYLOAD_FIELDS();
    K2_DEF_FMT(K23SITxnEndResponse);
};

} // ns skv::http::dto
