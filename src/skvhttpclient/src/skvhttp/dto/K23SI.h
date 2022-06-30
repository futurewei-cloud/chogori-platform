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
    std::vector<String> fieldsForPartialUpdate;

    K2_SERIALIZABLE_FMT(WriteRequest, timestamp, collectionName, schemaName, isDelete, precondition, value, fieldsForPartialUpdate);
};

struct WriteResponse {
    K2_SERIALIZABLE_FMT(WriteResponse);
};

struct CreateQueryRequest {
    Timestamp timestamp; // identify the issuing transaction
    String collectionName;
    String schemaName;
    // use the name "key" so that we can use common routing from CPO client
    SKVRecord::Storage key; // key for routing and will be interpreted as inclusive start key by the server
    SKVRecord::Storage endKey; // exclusive scan end key

    int32_t recordLimit = -1; // Max number of records server should return, negative is no limit
    bool includeVersionMismatch = false; // Whether mismatched schema versions should be included in results
    bool reverseDirection = false; // If true, key should be high and endKey low

    expression::Expression filterExpression; // the filter expression for this query
    std::vector<String> projection; // Fields by name to include in projection

    K2_SERIALIZABLE_FMT(CreateQueryRequest, timestamp, collectionName, schemaName, key, endKey, recordLimit,
        includeVersionMismatch, reverseDirection, filterExpression, projection);
};

struct CreateQueryResponse {
    uint64_t queryId{0};    // the query if the response was OK
    K2_SERIALIZABLE_FMT(CreateQueryResponse, queryId);
};

struct QueryRequest {
    Timestamp timestamp; // identify the issuing transaction
    uint64_t queryId{0};
    K2_SERIALIZABLE_FMT(QueryRequest, timestamp, queryId);
};

struct QueryResponse {
    bool done{false};
    std::vector<SKVRecord::Storage> records;
    K2_SERIALIZABLE_FMT(QueryResponse, done, records);
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
