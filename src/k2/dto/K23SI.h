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
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

#include "Collection.h"
#include "ControlPlaneOracle.h"
#include "SKVRecord.h"
#include "Timestamp.h"
#include "Expression.h"

namespace k2 {
namespace dto {

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
    uint64_t txnid = 0; // the transaction ID: random generated by client
    Timestamp timestamp; // the TSO timestamp of the transaction
    TxnPriority priority = TxnPriority::Medium;  // transaction priority: user-defined: used to pick abort victims by K2 (0 is highest)
    bool operator==(const K23SI_MTR& o) const;
    bool operator!=(const K23SI_MTR& o) const;
    size_t hash() const;
    K2_PAYLOAD_FIELDS(txnid, timestamp, priority);
    K2_DEF_FMT(K23SI_MTR, txnid, timestamp, priority);
};
// zero-value for MTRs
inline const K23SI_MTR K23SI_MTR_ZERO;

// Complete unique identifier of a transaction in the system
// Note that trh is used only for routing and debug purposes - the transaction is still
// identified completely by its MTR and so we consider two TxnIds to be equal if their MTRs are equal.
struct TxnId {
    // this is the routable key for the TR - we can route requests for the TR (i.e. PUSH)
    // based on the partition map of the collection.
    dto::Key trh;
    // the MTR for the transaction
    dto::K23SI_MTR mtr;

    size_t hash() const {
        // do not use trh in the hash as we want collision on two TxnIds with same MTR.
        return mtr.hash();
    }

    bool operator==(const TxnId& o) const{
        // two TxnIds are equal iff their MTRs are the same
        return mtr == o.mtr;
    }

    bool operator!=(const TxnId& o) const{
        return !operator==(o);
    }

    K2_PAYLOAD_FIELDS(trh, mtr);
    K2_DEF_FMT(TxnId, trh, mtr);
};

} // ns dto
} // ns d2

// Define std::hash for some objects so that we can use them in hash maps/sets
namespace std {
template <>
struct hash<k2::dto::K23SI_MTR> {
    size_t operator()(const k2::dto::K23SI_MTR& mtr) const {
        return mtr.hash();
    }
};  // hash

template <>
struct hash<k2::dto::TxnId> {
    size_t operator()(const k2::dto::TxnId& txnId) const {
        return txnId.hash();
    }
};  // hash

} // ns std

namespace k2 {
namespace dto {

// The core data needed by any 3SI record version, used by both write intents and
// committed records. It is split into a separate struct so that code can more easily
// be written to handle both write intents and committed records.
struct DataRecord {
    SKVRecord::Storage value;
    bool isTombstone = false;

    K2_PAYLOAD_FIELDS(value, isTombstone);
    K2_DEF_FMT(DataRecord, value, isTombstone);
};

// A write intent. This is separate from the DataRecord structure below which is used for
// committed records because a write intent needs to store more information.
struct WriteIntent {
    DataRecord data;
    TxnId txnId;

    // The request_id as given to the server by the client, it is used to
    // provide idempotent behavior in the case of retries
    uint64_t request_id = 0;

    WriteIntent() = default;
    WriteIntent(DataRecord&& d, TxnId txn, uint64_t id) : data(std::move(d)),
            txnId(std::move(txn)), request_id(id) {}

    K2_PAYLOAD_FIELDS(data, txnId, request_id);
    K2_DEF_FMT(WriteIntent, data, txnId, request_id);
};

// A committed record in the 3SI version cache.
struct CommittedRecord {
    DataRecord data;
    Timestamp timestamp;

    CommittedRecord() = default;
    CommittedRecord(DataRecord&& d, Timestamp t): data(std::move(d)), timestamp(t) {}

    K2_PAYLOAD_FIELDS(data, timestamp);
    K2_DEF_FMT(CommittedRecord, data, timestamp);
};


K2_DEF_ENUM(TxnRecordState,
        Created,         // The state in which all new TxnRecords are put when first created in memory
        InProgress,      // The txn InProgress has persisted
        ForceAborted,    // The txn ForceAbort has been persisted
        AbortedPIP,      // The txn has been Aborted and we're persisting the txn record
        Aborted,         // The txn Abort has been persisted
        CommittedPIP,    // The txn has been Committed and we're persisting the txn record
        Committed,       // The txn Commit has been persisted
        FinalizedPIP     // The txn has been Finalized and we're persisting the txn record
);

// The main READ DTO.
struct K23SIReadRequest {
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName; // the name of the collection
    K23SI_MTR mtr; // the MTR for the issuing transaction
    // use the name "key" so that we can use common routing from CPO client
    Key key; // the key to read

    K23SIReadRequest() = default;
    K23SIReadRequest(Partition::PVID p, String cname, K23SI_MTR _mtr, Key _key) :
        pvid(std::move(p)), collectionName(std::move(cname)), mtr(std::move(_mtr)), key(std::move(_key)) {}

    K2_PAYLOAD_FIELDS(pvid, collectionName, mtr, key);
    K2_DEF_FMT(K23SIReadRequest, pvid, collectionName, mtr, key);
};

// The response for READs
struct K23SIReadResponse {
    SKVRecord::Storage value; // the value we found
    K2_PAYLOAD_FIELDS(value);
    K2_DEF_FMT(K23SIReadResponse, value);
};

// status codes for reads
struct K23SIStatus {
    static const inline Status KeyNotFound=k2::Statuses::S404_Not_Found;
    static const inline Status RefreshCollection=k2::Statuses::S410_Gone;
    static const inline Status AbortConflict=k2::Statuses::S409_Conflict;
    static const inline Status AbortRequestTooOld=k2::Statuses::S403_Forbidden;
    static const inline Status OK=k2::Statuses::S200_OK;
    static const inline Status Created=k2::Statuses::S201_Created;
    static const inline Status ConditionFailed=k2::Statuses::S412_Precondition_Failed;
    static const inline Status OperationNotAllowed=k2::Statuses::S405_Method_Not_Allowed;
    static const inline Status BadParameter=k2::Statuses::S422_Unprocessable_Entity;
    static const inline Status BadFilterExpression=k2::Statuses::S406_Not_Acceptable;
    static const inline Status InternalError=k2::Statuses::S500_Internal_Server_Error;
    static const inline Status ServiceUnavailable=k2::Statuses::S503_Service_Unavailable;
};

struct K23SIWriteRequest {
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName; // the name of the collection
    K23SI_MTR mtr; // the MTR for the issuing transaction
    // The TRH key is used to find the K2 node which owns a transaction. It should be set to the key of
    // the first write (the write for which designateTRH was set to true)
    // Note that this is not an unique identifier for a transaction record - transaction records are
    // uniquely identified by the tuple (mtr, trh)
    Key trh;
    bool isDelete = false; // is this a delete write?
    bool designateTRH = false; // if this is set, the server which receives the request will be designated the TRH
    // Whether the server should reject the write if a previous version exists, like a SQL insert.
    // In the future we want more expressive preconditions, but those will be on the fields of a record
    // whereas this is the only record-level precondition that makes sense so it is its own flag
    bool rejectIfExists = false;
    // Generated on the client and stored on by the server so that
    uint64_t request_id;
    // use the name "key" so that we can use common routing from CPO client
    Key key; // the key for the write
    SKVRecord::Storage value; // the value of the write
    std::vector<uint32_t> fieldsForPartialUpdate; // if size() > 0 then this is a partial update

    K23SIWriteRequest() = default;
    K23SIWriteRequest(Partition::PVID _pvid, String cname, K23SI_MTR _mtr, Key _trh, bool _isDelete,
                      bool _designateTRH, bool _rejectIfExists, uint64_t id, Key _key, SKVRecord::Storage _value,
                      std::vector<uint32_t> _fields) :
        pvid(std::move(_pvid)), collectionName(std::move(cname)), mtr(std::move(_mtr)), trh(std::move(_trh)),
        isDelete(_isDelete), designateTRH(_designateTRH), rejectIfExists(_rejectIfExists), request_id(id),
        key(std::move(_key)), value(std::move(_value)), fieldsForPartialUpdate(std::move(_fields)) {}

    K2_PAYLOAD_FIELDS(pvid, collectionName, mtr, trh, isDelete, designateTRH, rejectIfExists, request_id, key, value, fieldsForPartialUpdate);
    K2_DEF_FMT(K23SIWriteRequest, pvid, collectionName, mtr, trh, isDelete, designateTRH, rejectIfExists, request_id, key, value, fieldsForPartialUpdate);
};

struct K23SIWriteResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SIWriteResponse);
};

struct K23SIQueryRequest {
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
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

struct K23SITxnHeartbeatRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    Partition::PVID pvid;
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
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SITxnHeartbeatResponse);
};

template <typename ValueType>
struct K23SI_PersistenceRequest {
    SerializeAsPayload<ValueType> value;  // the value of the write
    K2_PAYLOAD_FIELDS(value);
    K2_DEF_FMT(K23SI_PersistenceRequest);
};

struct K23SI_PersistenceResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SI_PersistenceResponse);
};

struct K23SI_PersistenceRecoveryRequest {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SI_PersistenceRecoveryRequest);
};

struct K23SI_PersistencePartialUpdate {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SI_PersistencePartialUpdate);
};

// we route requests to the TRH the same way as standard keys therefore we need pvid and collection name
struct K23SITxnPushRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    Partition::PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the incumbent.
    // use the name "key" so that we can use common routing from CPO client
    Key key;
    // the MTR for the incumbent transaction
    K23SI_MTR incumbentMTR;
    // the MTR for the challenger transaction
    K23SI_MTR challengerMTR;

    K2_PAYLOAD_FIELDS(pvid, collectionName, key, incumbentMTR, challengerMTR);
    K2_DEF_FMT(K23SITxnPushRequest, pvid, collectionName, key, incumbentMTR, challengerMTR);
};

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
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    Partition::PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the transaction to end.
    // use the name "key" so that we can use common routing from CPO client
    Key key;
    // the MTR for the transaction to end
    K23SI_MTR mtr;
    // the end action (Abort|Commit)
    EndAction action;
    // the keys this transaction wrote. We need to finalize these by converting write
    // intents to committed/aborted versions)
    std::vector<Key> writeKeys;

    // flag to tell if the server should finalize synchronously.
    // this is useful in cases where the client knows that the data from the txn will be accessed a lot after
    // the commit, so it may choose to wait in order to get better performance.
    // This flag does not impact correctness, just performance for certain workloads
    bool syncFinalize=false;
    // The interval from end to Finalize for a transaction
    Duration timeToFinalize{0};

    K2_PAYLOAD_FIELDS(pvid, collectionName, key, mtr, action, writeKeys, syncFinalize, timeToFinalize);
    K2_DEF_FMT(K23SITxnEndRequest, pvid, collectionName, key, mtr, action, syncFinalize, timeToFinalize, writeKeys);
};

struct K23SITxnEndResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SITxnEndResponse);
};

struct K23SITxnFinalizeRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    Partition::PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the transaction
    Key trh;
    // the MTR for the transaction
    K23SI_MTR mtr;
    // the key to finalize. The request is routed based on this key
    Key key;
    // should we abort or commit
    EndAction action;

    K2_PAYLOAD_FIELDS(pvid, collectionName, trh, mtr, key, action);
    K2_DEF_FMT(K23SITxnFinalizeRequest, pvid, collectionName, trh, mtr, key, action);
};

struct K23SITxnFinalizeResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SITxnFinalizeResponse);
};

struct K23SIPushSchemaRequest {
    String collectionName;
    Schema schema;
    K2_PAYLOAD_FIELDS(collectionName, schema);
    K2_DEF_FMT(K23SIPushSchemaRequest, collectionName, schema);
};

struct K23SIPushSchemaResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SIPushSchemaResponse);
};

} // ns dto
} // ns k2
