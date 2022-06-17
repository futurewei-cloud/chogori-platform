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

// stl
#include <cstdint> // for int types
#include <cstring> // for size_t types

// k2
#include <k2/logging/Log.h>
#include "Payload.h"
#include "RPCTypes.h"

namespace k2 {
namespace txconstants {

constexpr char K2RPCMAGIC = char('K') ^ char('2');

// No header of ours would ever exceed this size
constexpr uint8_t MAX_HEADER_SIZE = 128;

} // namespace txconstants

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
// | 4          | Checksum        | The optional checksum for the message
//
// Note that since the message is likely to be binaried, the payload will be stored and presented as
// a Payload, which is basically an iovec which exposes the binaries for the payload.

// The fixed message header
class FixedHeader {
public:
    char magic = txconstants::K2RPCMAGIC;
    uint8_t version = 0x1;
    // bitmap which indicates which variable fields below are set. The bitmap should be used to initialize
    // a MessageMetadata, and then use the API from MessageMetadata to determine what fields are set and
    // what their values are
    uint8_t features = 0x0;
    Verb verb = InternalVerbs::NIL;
};

// The variable message header. Fields here are only valid if set in the Features bitmap above
class MessageMetadata {
public: // API
    // PayloadSize at position 0
    void setPayloadSize(uint32_t payloadSize);
    bool isPayloadSizeSet() const;

    // RequestID at position 1
    void setRequestID(uint32_t requestID);
    bool isRequestIDSet() const;

    // ResponseID at position 2
    void setResponseID(uint32_t responseID);
    bool isResponseIDSet() const;

    // checksum at position 3
    void setChecksum(uint32_t checksum);
    bool isChecksumSet() const;

    // this method is used to determine how many wire bytes are needed given the set features
    size_t wireByteCount();

public: // fields
    uint8_t features = 0x0;
    uint32_t payloadSize = 0;
    uint32_t requestID = 0;
    uint32_t responseID = 0;
    uint32_t checksum = 0;
    // MAYBE TODO  crypto, sender endpoint
};
} // k2
