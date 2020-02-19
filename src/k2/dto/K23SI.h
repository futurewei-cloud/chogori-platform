#pragma once
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>

#include "Collection.h"
#include "TSO.h"
namespace k2 {
namespace dto {

struct K23SI_MTR {
    uint64_t txnid = 0;
    Timestamp timestamp;
    uint64_t priority = 0;
    bool operator==(const K23SI_MTR& o) const;
    bool operator!=(const K23SI_MTR& o) const;
    K2_PAYLOAD_FIELDS(txnid, timestamp, priority);
};
extern const K23SI_MTR K23SI_MTR_ZERO;

struct K23SI_TRH_ID {
    Key key;
    K2_PAYLOAD_FIELDS(key);
};

struct K23SIReadRequest {
    Partition::PVID partitionVID;
    String collectionName;
    K23SI_MTR mtr;
    Key key;
    K2_PAYLOAD_FIELDS(partitionVID, collectionName, mtr, key);
};

template<typename ValueType>
struct K23SIReadResponse {
    Key key;
    SerializeAsPayload<ValueType> value;
    uint64_t abortPriority = 0;
    K2_PAYLOAD_FIELDS(key, value, abortPriority);
};

template <typename ValueType>
struct K23SIWriteRequest {
    Partition::PVID partitionVID;
    String collectionName;
    K23SI_MTR mtr;
    K23SI_TRH_ID trh;
    Key key;
    SerializeAsPayload<ValueType> value;
    K2_PAYLOAD_FIELDS(partitionVID, collectionName, mtr, trh, key, value);
};

struct K23SIWriteResponse {
    uint64_t abortPriority = 0;
    K2_PAYLOAD_COPYABLE;
};

struct K23SITxnPushRequest {
    K2_PAYLOAD_COPYABLE;
};

struct K23SITxnPushResponse {
    K2_PAYLOAD_COPYABLE;
};

struct K23SITxnEndRequest {
    K2_PAYLOAD_COPYABLE;
};

struct K23SITxnEndResponse {
    K2_PAYLOAD_COPYABLE;
};

struct K23SITxnFinalizeRequest {
    K2_PAYLOAD_COPYABLE;
};

struct K23SITxnFinalizeResponse {
    K2_PAYLOAD_COPYABLE;
};

} // ns dto
} // ns k2
