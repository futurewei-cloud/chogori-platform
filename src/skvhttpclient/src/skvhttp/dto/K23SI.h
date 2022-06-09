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

K2_DEF_ENUM(ExistencePrecondition,
            None,
            Exists,
            NotExists);

// This is the end action to be taken on the transaction and its writes. Here are the current
// use cases for this:
// 1. Client library is ending a transaction(commit/abort)
// 2. TRH is finalizing the transaction at a participant
// 3. A PUSH response is signaling what action should be done to the WI which triggered the PUSH
K2_DEF_ENUM(EndAction,
            None,
            Abort,
            Commit);

struct TxnBeginRequest {
    TxnOptions options;
    K2_SERIALIZABLE_FMT(TxnBeginRequest, options);
};

struct TxnBeginResponse {
    Timestamp timestamp;
    K2_SERIALIZABLE_FMT(TxnBeginResponse, timestamp);
};

// The main READ DTO.
struct ReadRequest {
    Timestamp timestamp;    // identify the txn
    String collectionName;  // the name of the collection
    // use the name "key" so that we can use common routing from CPO client
    String schemaName;
    SKVRecord::Storage key;  // the key to read
    K2_SERIALIZABLE_FMT(ReadRequest, timestamp, collectionName, schemaName, key);
};

// The response for READs
struct ReadResponse {
    String collectionName;  // the name of the collection
    // use the name "key" so that we can use common routing from CPO client
    String schemaName;
    SKVRecord::Storage record;
    K2_SERIALIZABLE_FMT(ReadResponse, collectionName, schemaName, record);
};

struct WriteRequest {
    Timestamp timestamp; // identify the txn
    String collectionName; // the name of the collection
    String schemaName;     // name of the schema for the following storage value
    bool isDelete = false; // is this a delete write?
    // Whether the server should reject the write if a previous version exists (like a SQL insert),
    // or reject a write if a previous version does not exists (e.g. to know if a delete actually deleted
    // a record). In the future we want more expressive preconditions, but those will be on the fields of
    // a record whereas this is the only record-level precondition that makes sense so it is its own flag
    ExistencePrecondition precondition = ExistencePrecondition::None;

    SKVRecord::Storage value; // the value of the write. Contains a value for each field

    // if size() > 0 then this is a partial update, and this vector contains the indices of the new field values
    std::vector<uint32_t> fieldsForPartialUpdate;

    K2_SERIALIZABLE_FMT(WriteRequest, timestamp, collectionName, schemaName, isDelete, precondition, value, fieldsForPartialUpdate);
};

struct WriteResponse {
    K2_SERIALIZABLE_FMT(WriteResponse);
};

struct QueryRequest {
    Timestamp timestamp; // identify the issuing transaction
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

    K2_SERIALIZABLE_FMT(QueryRequest, timestamp, collectionName, key, endKey, exclusiveKey, recordLimit,
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

    Timestamp timestamp;  // identify the issuing transaction

    // The user must specify the inclusive start and exclusive end keys for the range scan, but the client
    // still needs to encode these keys so we use SKVRecords. The SKVRecords will be created with an
    // appropriate schema by the client createQuery function. The user is then expected to serialize the
    // key fields into the SKVRecords, similar to a single key read request.
    //
    // They must be a fully specified prefix of the key fields. For example, if the key fields are defined
    // as {ID, NAME, TIMESTAMP} then {ID = 1, TIMESTAMP = 10} is not a valid start or end scanRecord, but
    // {ID = 1, NAME = J} is valid.
    SKVRecord::Storage startScanRecord;
    SKVRecord::Storage endScanRecord;
    K2_SERIALIZABLE_FMT(Query, timestamp, startScanRecord, endScanRecord, done, inprogress, keysProjected, request);
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

struct TxnEndRequest {
    Timestamp timestamp; // identify the issuing transaction

    // the end action (Abort|Commit)
    EndAction action;

    K2_SERIALIZABLE_FMT(TxnEndRequest, timestamp, action);
};

struct TxnEndResponse {
    K2_SERIALIZABLE_FMT(TxnEndResponse);
};

} // ns skv::http::dto
