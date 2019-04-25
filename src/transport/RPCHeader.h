//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once

// stl
#include <cstdint> // for int types
#include <cstring> // for size_t types

// k2tx
#include "Log.h"

namespace k2tx {
static const uint8_t K2RPCMAGIC = uint8_t('K') ^ uint8_t('2');

// Header format (RPC Version = 0x1)
// | size(byte) | Description     | Comments
// |------------|-----------------|------------------------------------------------------------------

// fixed fields:
// | 1          | Magic           | Magic byte: 'K' ^ '2' = '01111001' = 0x79
// | 1          | Version         | RPC version. Indicates version of RPC used
// | 1          | Features        | Feature bitmap
// | 1          | Verb            | The message verb

// variable fields. Presense is based on feature vector
// | 4          | Payload Size    | Determines how many following bytes belong to payload
// | 4          | RequestID       | The request message ID - short-term unique number
// | 4          | ResponseID      | The response message ID - repeat from a previous msg.RequestID
//
// Note that since the message is likely to be fragmented, the payload will be stored and presented as
// a Payload, which is basically an iovec which exposes the fragments for the payload.

// The fixed message header
class FixedHeader {
public:
    uint8_t magic = K2RPCMAGIC;
    uint8_t version = 0x1;
    // bitmap which indicates which variable fields below are set. The bitmap should be used to initialize
    // a MessageMetadata, and then use the API from MessageMetadata to determine what fields are set and
    // what their values are
    uint8_t features = 0x0;
    uint8_t verb = 0x0;
};

// The variable message header. Fields here are only valid if set in the Features bitmap above
class MessageMetadata {
public: // API
    // RequestID at position 0
    void SetRequestID(uint32_t requestID) {
        K2DEBUG("Set request id=" << requestID);
        this->requestID = requestID;
        this->features |= (1<<0); // bit0
    }
    bool IsRequestIDSet() {
        K2DEBUG("is request id set=" << (this->features & (1<<0)) );
        return this->features & (1<<0); // bit0
    }

    // ResponseID at position 1
    void SetResponseID(uint32_t responseID) {
        K2DEBUG("Set response id=" << responseID);
        this->responseID = responseID;
        this->features |= (1<<1); // bit1
    }
    bool IsResponseIDSet() {
        K2DEBUG("is response id set=" << (this->features & (1<<1)) );
        return this->features & (1<<1); // bit1
    }

    // PayloadSize at position 2
    void SetPayloadSize(uint32_t payloadSize) {
        if (payloadSize > 0) {
            this->payloadSize = payloadSize;
            this->features |= (1<<2); // bit2
        }
    }
    bool IsPayloadSizeSet() {
        K2DEBUG("is payloadSize set=" << (this->features & (1<<2)) );
        return this->features & (1<<2); // bit2
    }

    // this method is used to determine how many wire bytes are needed given the set features
    size_t WireByteCount() {
        return IsPayloadSizeSet()*sizeof(payloadSize) +
               IsRequestIDSet()*sizeof(requestID) +
               IsResponseIDSet()*sizeof(responseID);
    }

public: // fields
    uint8_t features = 0x0;
    uint32_t payloadSize;
    uint32_t requestID;
    uint32_t responseID;
    // MAYBE TODO CRC, crypto, sender endpoint
};

} // k2tx