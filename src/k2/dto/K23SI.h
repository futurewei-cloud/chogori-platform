#pragma once
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

#include "Collection.h"
#include "TSO.h"
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

std::ostream& operator<<(std::ostream& os, const TxnPriority& pri) {
    const char* strpri = "bad priority";
    switch (pri) {
        case TxnPriority::Highest: strpri= "highest"; break;
        case TxnPriority::High: strpri= "high"; break;
        case TxnPriority::Medium: strpri= "medium"; break;
        case TxnPriority::Low: strpri= "low"; break;
        case TxnPriority::Lowest: strpri= "lowest"; break;
        default: break;
    }
    return os << "state=" << strpri;
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
    friend std::ostream& operator<<(std::ostream& os, const K23SI_MTR& mtr) {
        return os << "txnid=" << mtr.txnid << ", timestamp=" << mtr.timestamp << ", priority=" << mtr.priority;
    }
};
// zero-value for MTRs
extern const K23SI_MTR K23SI_MTR_ZERO;

// The main READ DTO.
struct K23SIReadRequest {
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName; // the name of the collection
    K23SI_MTR mtr; // the MTR for the issuing transaction
    Key key; // the key to read
    K2_PAYLOAD_FIELDS(pvid, collectionName, mtr, key);
};

// The response for READs
template<typename ValueType>
struct K23SIReadResponse {
    Key key; // the key of the record. TODO maybe this isn't needed?
    SerializeAsPayload<ValueType> value; // the value we found
    K2_PAYLOAD_FIELDS(key, value);
};

// status codes for reads
struct K23SIStatus {
    static Status KeyNotFound() { return k2::Status::S404_Not_Found();}
    static Status RefreshCollection() { return k2::Status::S410_Gone();}
    static Status AbortConflict() { return k2::Status::S409_Conflict();}
    static Status AbortRequestTooOld() { return k2::Status::S403_Forbidden();}
    static Status OK() { return k2::Status::S200_OK(); }
    static Status Created() { return k2::Status::S201_Created(); }
    static Status OperationNotAllowed() { return k2::Status::S405_Method_Not_Allowed(); }
};

template <typename ValueType>
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
    Key key; // the key for the write
    SerializeAsPayload<ValueType> value; // the value of the write
    K2_PAYLOAD_FIELDS(pvid, mtr, trh, value, collectionName, isDelete, designateTRH, key);
};

struct K23SIWriteResponse {
    Key key;  // the key for the write. TODO maybe this isn't needed?
    K2_PAYLOAD_FIELDS(key);
};

struct K23SITxnHeartbeatRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    Partition::PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the transaction we want to heartbeat
    Key trh;
    // the MTR for the transaction we want to heartbeat
    K23SI_MTR mtr;

    K2_PAYLOAD_FIELDS(pvid, collectionName, trh, mtr);
};

struct K23SITxnHeartbeatResponse {
    K2_PAYLOAD_COPYABLE;
};

struct K23SI_PersistenceRequest {
    K2_PAYLOAD_COPYABLE;
};

struct K23SI_PersistenceResponse {
    K2_PAYLOAD_COPYABLE;
};

// we route requests to the TRH the same way as standard keys therefore we need pvid and collection name
struct K23SITxnPushRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    Partition::PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the incumbent
    Key incumbentTrh;
    // the MTR for the incumbent transaction
    K23SI_MTR incumbentMTR;
    // the MTR for the challenger transaction
    K23SI_MTR challengerMTR;

    K2_PAYLOAD_FIELDS(pvid, collectionName, incumbentTrh, incumbentMTR, challengerMTR);
};

// Response for PUSH operation
struct K23SITxnPushResponse {
    // the mtr of the winning transaction
    K23SI_MTR winnerMTR;
    K2_PAYLOAD_FIELDS(winnerMTR);
};

enum class EndAction:uint8_t {
    Abort,
    Commit
};

struct K23SITxnEndRequest {
    // the partition version ID for the TRH. Should be coming from an up-to-date partition map
    Partition::PVID pvid;
    // the name of the collection
    String collectionName;
    // trh of the transaction to end
    Key trh;
    // the MTR for the transaction to end
    K23SI_MTR mtr;
    // the end action (Abort|Commit)
    EndAction action;

    K2_PAYLOAD_FIELDS(pvid, collectionName, trh, mtr, action);
};

struct K23SITxnEndResponse {
    // As a response to ending a transaction, we will notify what is the suggested minimum priority to use
    // if the user wants to retry the transaction
    TxnPriority minRetryPriority = TxnPriority::Lowest;

    K2_PAYLOAD_FIELDS(minRetryPriority);
};

} // ns dto
} // ns k2
