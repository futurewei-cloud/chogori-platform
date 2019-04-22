//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "Payload.h"

// stl
#include <cstring> // for memcpy

//k2tx
#include "Log.h"

namespace k2tx {

Payload::Payload(Allocator_t allocator, const String& protocol):
    _appendOffset(0),
    _totalCapacity(0),
    _allocator(allocator),
    _protocol(protocol) {
    K2DEBUG("ctor for proto: " << protocol);
}

void Payload::Append(const Binary_t* data, size_t length) {
    K2DEBUG("append size=" << length);
    if (_allocator == nullptr) {
        K2ERROR("Cannot append into a non-allocating payload");
        throw NonAllocatingPayloadException();
    }
    this->Write(data, length, this->_appendOffset);
}

void Payload::Write(const Binary_t* data, size_t length, size_t offset) {
    K2DEBUG("write size=" << length << ", offset="<< offset);
    if (length == 0) {
        return; // nothing to write so no-op
    }

    // make sure we have enough capacity
    // we depend on this method call to throw if we have to allocate new packets but have null allocator
    _ensureCapacity(offset, length);

    // assume we're appending (i.e. the easy case)
    // the index for the fragment where we should start writing.
    auto fidx = _fragments.size() - 1;
    // find the offset inside the fragment where we should write
    auto fragmentWriteOffset = LastFragmentSize();

    if (offset != _appendOffset) {
        K2DEBUG("Potential non-append use case"); // a.k.a. random write somewhere in the middle

        // the base offset for the last fragment
        auto fragmentBaseOffset = _totalCapacity - _fragments[fidx].size();

        // we need to go back until we find the fragment which contains our desired offset
        while(fragmentBaseOffset > offset) {
            --fidx;
            fragmentBaseOffset -= _fragments[fidx].size();
        }

        // we found which fragment we need: _fragments[fidx]
        // we found the capacity up to (but not including) this fragment: fragmentBaseOffset
        // and now adjust the offset within this fragment where we should write
        fragmentWriteOffset = offset - fragmentBaseOffset;
    }
    K2DEBUG("fidx=" << fidx << ", writeOffset=" << fragmentWriteOffset);

    // set the new append offset if needed. It is possible we're just overwriting a part of the buffer
    _appendOffset = std::max(_appendOffset, offset + length);
    K2DEBUG("new append offset will be=" << _appendOffset);

    while (1) {
        auto& fragment = _fragments[fidx];
        // determine how many bytes we can copy inside this fragment
        auto canCopy = std::min(length, fragment.size() - fragmentWriteOffset);
        K2DEBUG("canCopy=" << canCopy << ", length=" << length <<", left=" << (length-canCopy));

        // copy what we can into this fragment
        std::memcpy(fragment.get_write() + fragmentWriteOffset, data, canCopy);
        length -= canCopy; // we now have `canCopy` fewer bytes left to copy
        if (length == 0) {
            break; // done copying
        }
        data += canCopy; // we've copied the front `canCopy` bytes from data. rewind data pointer
        ++fidx; // we've potentially consumed this fragment. Move on to next one
        fragmentWriteOffset = 0; // if this write needs more packets, we'll always write from beginning of packet
    }
}

void Payload::_ensureCapacity(size_t offset, size_t length) {
    K2DEBUG("ensureCapacity offset=" << offset << ", length=" << length);

    if (length == 0) {
        return; // nothing to write so no-op
    }
    size_t endOffset = offset + length - 1; // the incoming write will end at this offset
    K2DEBUG("ensureCapacity endoffset=" << endOffset << ", capacity=" << _totalCapacity);

    if (_allocator == nullptr && endOffset >= _totalCapacity) {
        K2ERROR("Cannot allocate capacity in a non-allocating payload");
        throw NonAllocatingPayloadException();
    }

    while (endOffset >= _totalCapacity) { // we need capacity to satisfy the end offset
        auto fragment = _allocator(); // allocate a fragment
        _totalCapacity += fragment.size();
        _fragments.push_back(std::move(fragment));
    }
}

void Payload::CopyInto(Binary_t* dst) {
    K2DEBUG("Copying into buffer");
    if (Size() > 0) {
        // write all but last fragment as-is
        for (size_t i = 0; i < _fragments.size() - 1; ++i) {
            std::memcpy(dst, _fragments[i].get(), _fragments[i].size());
            dst += _fragments[i].size(); // rewind the write pointer
        }
        // we only write LastFragmentSize bytes from the last fragment
        std::memcpy(dst, _fragments.back().get(), LastFragmentSize());
    }
}

void Payload::AppendFragment(Fragment fragment) {
    K2DEBUG("append fragment of size=" << fragment.size());
    _appendOffset += fragment.size();
    _totalCapacity += fragment.size();
    _fragments.push_back(std::move(fragment));
}

} // k2tx
