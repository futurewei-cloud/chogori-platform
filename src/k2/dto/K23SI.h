#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>

#include "Collection.h"
#include "TSO.h"
namespace k2 {
namespace dto {

struct K23SI_MTR {
    uint64_t txnid;
    Timestamp timestamp;
    uint64_t priority;
    K2_PAYLOAD_FIELDS(txnid, timestamp, priority);
};

struct K23SI_TRH_ID {
    Key key;
    K2_PAYLOAD_FIELDS(key);
};

struct K23SIReadRequest {
    K23SI_MTR mtr;
    Key key;
    K2_PAYLOAD_FIELDS(mtr, key);
};

template<typename ValueType>
struct K23SIReadResponse {
    Key key;
    SerializeAsPayload<ValueType> value;
    uint64_t abortPriority;
    K2_PAYLOAD_FIELDS(key, value);
};

template <typename ValueType>
struct K23SIWriteRequest {
    K23SI_MTR mtr;
    K23SI_TRH_ID trh;
    Key key;
    SerializeAsPayload<ValueType> value;
    K2_PAYLOAD_FIELDS(mtr, trh, key, value);
};

struct K23SIWriteResponse {
    uint64_t abortPriority;
    K2_PAYLOAD_COPYABLE;
};

struct K23SIPushRequest {
    K2_PAYLOAD_COPYABLE;
};

struct K23SIPushResponse {
    K2_PAYLOAD_COPYABLE;
};

} // ns dto
} // ns k2
