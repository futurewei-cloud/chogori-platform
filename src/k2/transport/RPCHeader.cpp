#include "RPCHeader.h"

namespace k2 {

void MessageMetadata::setPayloadSize(uint32_t payloadSize) {
    K2DEBUG("Set payload size=" << payloadSize);
    if (payloadSize > 0) {
        this->payloadSize = payloadSize;
        this->features |= (1 << 0);  // bit0
    }
}

bool MessageMetadata::isPayloadSizeSet() const {
    K2DEBUG("is payloadSize set=" << (this->features & (1 << 0)));
    return this->features & (1 << 0);  // bit0
}

void MessageMetadata::setRequestID(uint32_t requestID) {
    K2DEBUG("Set request id=" << requestID);
    this->requestID = requestID;
    this->features |= (1 << 1);  // bit1
}

bool MessageMetadata::isRequestIDSet() const {
    K2DEBUG("is request id set=" << (this->features & (1 << 1)));
    return this->features & (1 << 1);  // bit1
}

void MessageMetadata::setResponseID(uint32_t responseID) {
    K2DEBUG("Set response id=" << responseID);
    this->responseID = responseID;
    this->features |= (1 << 2);  // bit2
}

bool MessageMetadata::isResponseIDSet() const {
    K2DEBUG("is response id set=" << (this->features & (1 << 2)));
    return this->features & (1 << 2);  // bit2
}

void MessageMetadata::setChecksum(uint32_t checksum) {
    K2DEBUG("Set checksum=" << checksum);
    this->checksum = checksum;
    this->features |= (1 << 3);  // bit3
}

bool MessageMetadata::isChecksumSet() const {
    K2DEBUG("is checksum set=" << (this->features & (1 << 3)));
    return this->features & (1 << 3);  // bit3
}

size_t MessageMetadata::wireByteCount() {
    return isPayloadSizeSet() * sizeof(payloadSize) +
            isRequestIDSet() * sizeof(requestID) +
            isResponseIDSet() * sizeof(responseID) +
            isChecksumSet() * sizeof(checksum);
}

} // namespace k2
