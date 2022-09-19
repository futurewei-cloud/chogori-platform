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

#include "Payload.h"
#include <crc32c/crc32c.h>

namespace k2 {

Payload::PayloadPosition::PayloadPosition():PayloadPosition(0, 0, 0) {
}

Payload::PayloadPosition::PayloadPosition(_Size bIdx, _Size bOff, size_t offset):
    bufferIndex(bIdx), bufferOffset(bOff), offset(offset) {
    // By convention, a position pointing to a non-existent location will point
    // to a bufferIndex which doesn't exist.
    // Empty payload will have position of (bidx=0, boff=0, off=0)
}

Payload::Payload(BinaryAllocator&& allocator):
    _size(0), _capacity(0), _allocator(std::move(allocator)) {
}

Payload::Payload(std::vector<Binary>&& externallyAllocatedBuffers, size_t containedDataSize):
    _buffers(std::move(externallyAllocatedBuffers)),
    _size(containedDataSize),
    _capacity(_size),
    _allocator(BinaryAllocator()) {
}

Payload::Payload():
    _size(0),
    _capacity(0),
    _allocator(BinaryAllocator()) {
}

BinaryAllocator Payload::DefaultAllocator(size_t default_size) {
    return BinaryAllocator(default_size, [](size_t bsize) { return Binary(bsize); });
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
    if (totalCapacity <= _capacity) return;
    // we're asked to make sure there is certain total capacity. Make sure we have it allocated
    while (_capacity < totalCapacity) {
        bool canAllocate = _allocateBuffer();
        K2ASSERT(log::tx, canAllocate, "unable to increase capacity");
    }
    // NB, if our cursor was past the end of the payload before this call, it will now be valid
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
    K2ASSERT(log::tx, !_allocator.canAllocate(), "cannot append to non-allocating payload");
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
    skip(offset - _currentPosition.offset);
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
        skip(needToCopySize);
    }

    return true;
}

bool Payload::read(Binary& binary) {
    size_t size;
    if (!read(size)) {
        return false;
    }

    if (getDataRemaining() < size) {
        return false;
    }

    Binary& buffer = _buffers[_currentPosition.bufferIndex];
    size_t currentBufferRemaining = buffer.size() - _currentPosition.bufferOffset;
    if (currentBufferRemaining < size) {        // this read spans multiple buffers
        char* data_p = (char*) malloc(size);
        if (!data_p) {
            return false;
        }
        // we want to read a total of `size` bytes which we know span multiple buffers.
        // delegate to the raw read() which copies data that could span.
        if (!read(data_p, size)) {  // execute a raw copy
            return false;
        }
        binary = Binary(data_p, size, seastar::make_free_deleter(data_p));
        return true;
    }

    // the read is inside a single binary - we can just share in no-copy way
    binary = buffer.share(_currentPosition.bufferOffset, size);
    skip(size);
    return true;
}

bool Payload::read(char& b) {
    if (getDataRemaining() == 0) return false;

    b = _buffers[_currentPosition.bufferIndex][_currentPosition.bufferOffset];
    skip(1);
    return true;
}

bool Payload::read(String& value) {
    _Size size;
    if (!read(size)) return false;
    value.resize(size - 1);  // the resulting string's size will be one less than what we read since '\0' doesn't count
    return read((void*)value.data(), size);
}

bool Payload::read(boost::multiprecision::cpp_dec_float_50& value) {
    String data;
    bool success = read(data);
    if (!success) return false;
    // construct from a c string.
    boost::multiprecision::cpp_dec_float_50 _val(data.c_str()); 
    value = _val; // NOT SURE
    return true;
}

bool Payload::read(boost::multiprecision::cpp_dec_float_100& value) {
    String data;
    bool success = read(data);
    if (!success) return false;
    // construct from a c string.
    boost::multiprecision::cpp_dec_float_100 _val(data.c_str());
    value = _val; // NOT SURE
    return true;
}

bool Payload::read(Payload& other) {
    size_t size;
    if (!read(size) || getDataRemaining() < size) return false;
    other.clear();
    other._size = size;
    other._capacity = size;
    other._allocator = BinaryAllocator();
    other._currentPosition = PayloadPosition();
    while(size > 0) {
        auto shared = _buffers[_currentPosition.bufferIndex].share();
        shared.trim_front(_currentPosition.bufferOffset);
        size_t currentBufferRemaining = shared.size();
        size_t trimSize = std::min(size, currentBufferRemaining);

        shared.trim(trimSize);
        other._buffers.push_back(std::move(shared));
        size -= trimSize;
        skip(trimSize);
    }
    return true;
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
        src.skip(needToCopySize);
        toCopy -= needToCopySize;
    }

    return true;
}

void Payload::reserve(size_t advance) {
    ensureCapacity(_currentPosition.offset + advance);
    skip(advance);
}

void Payload::truncateToCurrent() {
    if (_size == 0) return; // nothing to do
    _size = _currentPosition.offset;

    if (_currentPosition.bufferIndex == _buffers.size()) return; // we're past the end already

    // make the current position the end, and place our cursor just past the end

    // drop any extra buffers we might have
    _buffers.resize(_currentPosition.bufferIndex + 1);
    // we want our capacity to be now exactly the same as our size
    _capacity = _size;
    // trim the last buffer to contain exactly the data we have so far
    _buffers[_currentPosition.bufferIndex].trim(_currentPosition.bufferOffset);
    // finally, adjust our cursor so that it points correctly past the end of the payload
    ++_currentPosition.bufferIndex;
    _currentPosition.bufferOffset = 0;
}

void Payload::write(char b) {
    ensureCapacity(_currentPosition.offset + 1);
    _buffers[_currentPosition.bufferIndex].get_write()[_currentPosition.bufferOffset] = b;
    skip(1);
}

void Payload::write(const void* data, size_t size) {
    ensureCapacity(_currentPosition.offset + size);

    while (size > 0) {
        Binary& buffer = _buffers[_currentPosition.bufferIndex];
        size_t currentBufferRemaining = buffer.size() - _currentPosition.bufferOffset;
        size_t needToCopySize = std::min(size, currentBufferRemaining);

        std::memcpy(buffer.get_write() + _currentPosition.bufferOffset, data, needToCopySize);
        skip(needToCopySize);
        data = (void*)((char*)data + needToCopySize);
        size -= needToCopySize;
    }
}

void Payload::write(const String& value) {
    _Size size = value.size() + 1; // count the null character too
    write(size);
    write(value.data(), size);
}

void Payload::write(const boost::multiprecision::cpp_dec_float_50& value) {
    String ss(value.str()); // serialize as a String
    write(ss); // NOT SURE
}

void Payload::write(const boost::multiprecision::cpp_dec_float_100& value) {
    String ss(value.str()); // serialize as a String
    write(ss); // NOT SURE
}

void Payload::write(const Binary& bin) {
    write(bin.size());
    write(bin.get(), bin.size());
}

void Payload::write(const Payload& other) {
    // we only support this write at the end of an existing payload (append)
    K2ASSERT(log::tx, getDataRemaining() == 0, "cannot write a payload in the middle of another payload");

    size_t size = other.getSize();
    // write out how many bytes are following
    write(size);

    // reset ourselves so that we are exactly as big as the data we're currently holding
    // truncate to the current cursor
    truncateToCurrent();

    auto shared = const_cast<Payload*>(&other)->shareAll();
    for (size_t i= 0; i < shared._buffers.size() && size > 0; ++i) {
        // now we can extend our buffer list with shared buffers from the other payload
        // note that the share() call gives us a payload trimmed to contain exactly the data it should (size==capacity)
        auto& buf = shared._buffers[i];
        auto toMove = std::min(size, buf.size());
        if (toMove == 0) continue;
        buf.trim(toMove);

        _buffers.push_back(std::move(buf));
        _size += toMove;
        _capacity += toMove;
        size -= toMove;
        skip(toMove);
    }
}

void Payload::writeMany() {
    // this is needed for the base case of the recursive template version
}

size_t Payload::getFieldsSize() {
    return 0;
}

bool Payload::_allocateBuffer() {
    K2ASSERT(log::tx, _allocator.canAllocate(), "cannot allocate buffer without allocator");
    Binary buf = _allocator.allocate();
    if (!buf) {
        return false;
    }
    K2ASSERT(log::tx, buf.size() <= std::numeric_limits<_Size>::max() && buf.size() > 0, "invalid buffer size");
    // we're about to add one more in. Make sure we have room to add it
    K2ASSERT(log::tx, _buffers.size() < std::numeric_limits<_Size>::max(), "invalid number of buffers");

    _capacity += buf.size();
    _buffers.push_back(std::move(buf));
    return true;
}

void Payload::skip(size_t numBytes) {
    auto newOffset = numBytes + _currentPosition.offset;
    K2ASSERT(log::tx, newOffset <= _capacity, "offset must be within the existing payload");

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
    while (numBytes > 0) {
        auto canAdvance = std::min(numBytes,
                                   _buffers[_currentPosition.bufferIndex].size() - _currentPosition.bufferOffset);
        _currentPosition.offset += canAdvance;
        _currentPosition.bufferOffset += canAdvance;
        if (_currentPosition.bufferOffset == _buffers[_currentPosition.bufferIndex].size()) {
            // we hit past the end of the current buffer. Reset pointer to index 0 of next buffer
            _currentPosition.bufferOffset = 0;
            ++_currentPosition.bufferIndex;
        }
        numBytes -= canAdvance;
    }
}

uint32_t Payload::computeCrc32c() {
    // remember where we started
    auto curpos = getCurrentPosition();

    uint32_t checksum = 0;
    size_t size = getSize() - curpos.offset;

    while (size > 0) {
        const Binary& buffer = _buffers[_currentPosition.bufferIndex];
        size_t currentBufferRemaining = buffer.size() - _currentPosition.bufferOffset;
        size_t needToCopySize = std::min(size, currentBufferRemaining);

        checksum = crc32c::Extend(checksum, reinterpret_cast<const uint8_t*>(buffer.get() + _currentPosition.bufferOffset), needToCopySize);

        size -= needToCopySize;
        skip(needToCopySize);
    }

    // put the cursor back where it was before we started
    seek(curpos);
    return checksum;
}


Payload Payload::shareAll() {
    return shareRegion(0, getSize());
}

Payload Payload::shareRegion(size_t startOffset, size_t nbytes){
    auto previousPosition = getCurrentPosition();
    seek(startOffset);
    nbytes = std::min(_size - _currentPosition.offset, nbytes);

    Payload shared(std::move(BinaryAllocator(_allocator)));
    shared._size = nbytes;
    shared._capacity = nbytes; // the capacity of the new payload stops with the current data written

    size_t toShare = nbytes;
    size_t curBufIndex = _currentPosition.bufferIndex;
    size_t curBufOffset = _currentPosition.bufferOffset;
    while(toShare > 0) {
        auto& curBuf = _buffers[curBufIndex];
        auto shareSizeFromCurBuf = std::min(toShare, curBuf.size()-curBufOffset);
        auto fullSharedBuf = curBuf.share();

        // only share exactly what we need for the new payload
        fullSharedBuf.trim_front(curBufOffset);
        fullSharedBuf.trim(shareSizeFromCurBuf);
        shared._buffers.push_back(std::move(fullSharedBuf));
        toShare -= shareSizeFromCurBuf;
        curBufIndex++;
        curBufOffset = 0;
    }

    seek(previousPosition);
    return shared;
}

Payload Payload::copy(BinaryAllocator&& allocator) {
    Payload copied;
    Binary b = allocator.allocate(_size);

    // copy exactly the data we need
    size_t toCopy = _size;
    size_t curBufIndex = 0;
    while (toCopy > 0) {
        auto& curBuf = _buffers[curBufIndex];
        auto copySizeFromCurBuf = std::min(toCopy, curBuf.size());

        std::memcpy(b.get_write() + (b.size() - toCopy), curBuf.get(), copySizeFromCurBuf);
        toCopy -= copySizeFromCurBuf;
        curBufIndex++;
    }

    copied.appendBinary(std::move(b));
    copied._allocator = std::move(allocator);
    return copied;
}

Payload Payload::copy() {
    return copy(DefaultAllocator());
}

bool Payload::operator==(const Payload& o) const {
    Payload* me = const_cast<Payload*>(this);
    Payload* they = const_cast<Payload*>(&o);
    auto myPos = me->getCurrentPosition();
    auto theirPos = they->getCurrentPosition();
    me->seek(0);
    they->seek(0);
    auto mychksum = me->computeCrc32c();
    auto otherchksum = they->computeCrc32c();
    me->seek(myPos);
    they->seek(theirPos);
    return mychksum == otherchksum;
}


bool Payload::read(Duration& dur) {
    if (typeid(std::remove_reference<decltype(dur)>::type::rep) != typeid(long int)) {
        return false;
    }
    long int ticks = 0;
    if (!read(ticks)) return false;
    dur = Duration(ticks);
    return true;
}

void Payload::write(const Duration& dur) {
    write(dur.count());                 // write the tick count
}

} // namespace k2
