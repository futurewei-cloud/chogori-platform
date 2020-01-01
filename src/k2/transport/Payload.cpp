#include "Payload.h"

namespace k2 {

Payload::PayloadPosition::PayloadPosition():PayloadPosition(0, 0, 0) {
}

Payload::PayloadPosition::PayloadPosition(_Size bIdx, _Size bOff, size_t offset):
    bufferIndex(bIdx), bufferOffset(bOff), offset(offset) {
}

Payload::Payload(BinaryAllocatorFunctor allocator):
    _size(0), _capacity(0), _allocator(allocator) {
}

Payload::Payload(std::vector<Binary>&& externallyAllocatedBuffers, size_t containedDataSize):
    _buffers(std::move(externallyAllocatedBuffers)),
    _size(containedDataSize),
    _capacity(_size),
    _allocator(nullptr) {
}

Payload::Payload():
    _size(0),
    _capacity(0),
    _allocator(nullptr) {
}

bool Payload::isEmpty() const {
    return _size == 0;
}

size_t Payload::getSize() const {
    return _size;
}

size_t Payload::getCapacity() const {
    return _capacity;
}

void Payload::ensureCapacity(size_t totalCapacity) {
    while (getCapacity() < totalCapacity) {
        bool canAllocate = _allocateBuffer();
        K2ASSERT(canAllocate, "unable to increase capacity");
    }
}

std::vector<Binary> Payload::release() {
    std::vector<Binary> result(std::move(_buffers));
    clear();
    return result;
}

void Payload::clear() {
    _buffers.resize(0);
    _size = 0;
    _capacity = 0;
}

void Payload::appendBinary(Binary&& binary) {
    // we can only append into a non-self-allocating payload
    K2ASSERT(_allocator == nullptr, "cannot append to non-allocating payload");
    _size += binary.size();
    _capacity += binary.size();
    _buffers.push_back(std::move(binary));
}

void Payload::seek(size_t offset) {
    // grow if needed
    ensureCapacity(offset);

    // set the current position
    if (offset < _currentPosition.offset) {
        // don't bother searching backwards for now
        // Just start at 0 and go up
        _currentPosition = PayloadPosition();
    }
    _advancePosition(offset - _currentPosition.offset);
}

void Payload::seek(PayloadPosition position) {
    // grow if needed
    ensureCapacity(position.offset);

    // just remember the given position as our position
    _currentPosition = position;
}

Payload::PayloadPosition Payload::getCurrentPosition() const {
    return _currentPosition;
}

size_t Payload::getDataRemaining() const {
    // this is the data remaining to be read. It depends on where we're at in the payload
    return _size - _currentPosition.offset;
}

bool Payload::read(void* data, size_t size) {
    if (getDataRemaining() < size) {
        return false;
    }
    while (size > 0) {
        const Binary& buffer = _buffers[_currentPosition.bufferIndex];
        size_t currentBufferRemaining = buffer.size() - _currentPosition.bufferOffset;
        size_t needToCopySize = std::min(size, currentBufferRemaining);

        std::memcpy(data, buffer.get() + _currentPosition.bufferOffset, needToCopySize);
        size -= needToCopySize;
        data = (void*)((char*)data + needToCopySize);
        _advancePosition(needToCopySize);
    }

    return true;
}

bool Payload::read(Binary& binary, size_t size) {
    if (getDataRemaining() < size || binary.size() < size) {
        return false;
    }

    Binary& buffer = _buffers[_currentPosition.bufferIndex];
    size_t currentBufferRemaining = buffer.size() - _currentPosition.bufferOffset;
    if (currentBufferRemaining < size) {        // this read spans multiple buffers
        return read(binary.get_write(), size);  // execute a raw copy
    }

    // the read is inside a single binary - we can just share in no-copy way
    binary = buffer.share(_currentPosition.bufferOffset, size);
    _advancePosition(size);
    return true;
}

bool Payload::read(char& b) {
    if (getDataRemaining() == 0) return false;

    b = _buffers[_currentPosition.bufferIndex][_currentPosition.bufferOffset];
    _advancePosition(1);
    return true;
}

bool Payload::read(String& value) {
    _Size size;
    if (!read(size)) return false;
    value.resize(size);
    return read((void*)value.data(), size);
}

bool Payload::readMany() {
    return true;
}

bool Payload::copyFromPayload(Payload& src, size_t toCopy) {
    if (src.getDataRemaining() < toCopy) {
        return false; // not enough bytes in source
    }
    ensureCapacity(_currentPosition.offset + toCopy);

    while (toCopy > 0) {
        const Binary& buffer = src._buffers[src._currentPosition.bufferIndex];
        size_t currentBufferRemaining = buffer.size() - src._currentPosition.bufferOffset;
        size_t needToCopySize = std::min(toCopy, currentBufferRemaining);

        write(buffer.get() + src._currentPosition.bufferOffset, needToCopySize);
        _advancePosition(needToCopySize);
        src._advancePosition(needToCopySize);
        toCopy -= needToCopySize;
    }

    return true;
}

void Payload::skip(size_t advance) {
    ensureCapacity(_currentPosition.offset + advance);
    _advancePosition(advance);
}

void Payload::truncateToCurrent() {
    _size = _currentPosition.offset + 1;
}

void Payload::write(char b) {
    ensureCapacity(_currentPosition.offset + 1);
    _buffers[_currentPosition.bufferIndex].get_write()[_currentPosition.bufferOffset] = b;
    _advancePosition(1);
}

void Payload::write(const void* data, size_t size) {
    ensureCapacity(_currentPosition.offset + size);

    while (size > 0) {
        Binary& buffer = _buffers[_currentPosition.bufferIndex];
        size_t currentBufferRemaining = buffer.size() - _currentPosition.bufferOffset;
        size_t needToCopySize = std::min(size, currentBufferRemaining);

        std::memcpy(buffer.get_write() + _currentPosition.bufferOffset, data, needToCopySize);
        _advancePosition(needToCopySize);
        data = (void*)((char*)data + needToCopySize);
        size -= needToCopySize;
    }
}

void Payload::write(const String& value) {
    _Size size = value.size();
    write(size);
    write(value.data(), value.size());
}

void Payload::writeMany() {
}

bool Payload::_allocateBuffer() {
    K2ASSERT(_allocator, "cannot allocate buffer without allocator");
    Binary buf = _allocator();
    if (!buf) {
        return false;
    }
    assert(buf.size() <= std::numeric_limits<_Size>::max());
    // we're about to add one more in. Make sure we have room to add it
    assert(_buffers.size() < std::numeric_limits<_Size>::max());

    _capacity += buf.size();
    _buffers.push_back(std::move(buf));
    return true;
}

void Payload::_advancePosition(size_t advance) {
    auto newOffset = advance + _currentPosition.offset;
    K2ASSERT(newOffset <= _capacity, "offset must be within the existing payload");
    _size = std::max(_size, newOffset);
    if (newOffset == _capacity) {
        // append use case when we don't have the actual buffer allocated yet
        // allow +1 append and point to a non-existing new buffer
        _currentPosition.bufferOffset = 0;
        _currentPosition.offset = newOffset;
        _currentPosition.bufferIndex = _buffers.size();
        return;
    }

    // the newoffset should now be within the existing memory. Find where it falls
    while (advance > 0) {
        auto canAdvance = std::min(advance,
                                   _buffers[_currentPosition.bufferIndex].size() - _currentPosition.bufferOffset);
        _currentPosition.offset += canAdvance;
        _currentPosition.bufferOffset += canAdvance;
        if (_currentPosition.bufferOffset == _buffers[_currentPosition.bufferIndex].size()) {
            // we hit past the end of the current buffer. Reset pointer to index 0 of next buffer
            _currentPosition.bufferOffset = 0;
            ++_currentPosition.bufferIndex;
        }
        advance -= canAdvance;
    }
}

} // namespace k2
