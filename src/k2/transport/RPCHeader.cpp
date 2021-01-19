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

#include "RPCHeader.h"

namespace k2 {

void MessageMetadata::setPayloadSize(uint32_t payloadSize) {
    if (payloadSize > 0) {
        this->payloadSize = payloadSize;
        this->features |= (1 << 0);  // bit0
    }
}

bool MessageMetadata::isPayloadSizeSet() const {
    return this->features & (1 << 0);  // bit0
}

void MessageMetadata::setRequestID(uint32_t requestID) {
    this->requestID = requestID;
    this->features |= (1 << 1);  // bit1
}

bool MessageMetadata::isRequestIDSet() const {
    return this->features & (1 << 1);  // bit1
}

void MessageMetadata::setResponseID(uint32_t responseID) {
    this->responseID = responseID;
    this->features |= (1 << 2);  // bit2
}

bool MessageMetadata::isResponseIDSet() const {
    return this->features & (1 << 2);  // bit2
}

void MessageMetadata::setChecksum(uint32_t checksum) {
    this->checksum = checksum;
    this->features |= (1 << 3);  // bit3
}

bool MessageMetadata::isChecksumSet() const {
    return this->features & (1 << 3);  // bit3
}

size_t MessageMetadata::wireByteCount() {
    return isPayloadSizeSet() * sizeof(payloadSize) +
            isRequestIDSet() * sizeof(requestID) +
            isResponseIDSet() * sizeof(responseID) +
            isChecksumSet() * sizeof(checksum);
}

} // namespace k2
