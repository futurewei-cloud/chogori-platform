//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
// stl
#include <vector>
#include <functional>

// k2tx
#include "Fragment.h"
#include "BaseTypes.h"

namespace k2tx {

// Payload allows users of tranport to send and receive their messages. A payload consists of a list
// of fragments(think packets). The focus is on efficient, delivery of data in and out of the network interface.
class Payload {
public:
    // we throw this exception when we are asked to allocate more capacity but were constructed with null allocator
    // e.g. on append, or write beyond end of capacity
    class NonAllocatingPayloadException : public std::exception {};

public:
    // Construct a new payload with the given allocator and set the transport type in which it belongs.
    // It is possible to create payloads which cannot dynamically grow by passing a null allocator.
    // capacity has to be added manually by calling AppendFragment.
    Payload(Allocator_t allocator, const String& transportType);

    // Returns the fragments which currently span this payload. The fragments could be of different size.
    // NB the last packet is expected to have size() potentially larger than the data it is holding. For example,
    // if you append 1 byte in a 8K MTU system, you will get one fragment of size 8K.
    // The caller can use the helper method LastFragmentSize() to find out just how many bytes of data there are
    // in the last fragment
    // It is up to caller to read only the bytes they should from the last fragment:
    // auto lastDataPtr = payload.fragments.back().get();
    // auto lastDataSize = payload.LastFragmentSize();
    std::vector<Fragment>& Fragments() {
        return _fragments;
    }

    // Append(copy) the given data(with given length) to the end of this payload. The payload will allocate new fragments as needed
    void Append(const Binary_t* data, size_t length);

    // This API can be used to append an entire fragment. This is useful for building up a payload from fragment
    // slices
    void AppendFragment(Fragment fragment);

    // Write(copy) the given data(with given length) at the given offset. The payload will allocate sufficient fragments to
    // cover the necessary length.
    // For example, say fragment size is 8K and we currently have two fragments and 12K data. If you append 1K at offset=20K,
    // we will allocate one more fragment, and write the 1K into offset 4K of that fragment (global offset will be 20K).
    // That will make the end-of-file marker be 21K (and size of this payload also be 21K), and a call to Append afterwards
    // will append at marker=21K
    void Write(const Binary_t* data, size_t length, size_t offset);

    // Returns the size of the entire payload in number of bytes
    size_t Size() const { return _appendOffset;}

    // helper method, which should be used to determine how much data(number of bytes) there is in the last fragment
    size_t LastFragmentSize() const {
        if (_fragments.size() == 0) {
            return 0;
        }
        // see how much free space there is in the last fragment
        auto lastFragmentFreeData = _totalCapacity - _appendOffset;
        return _fragments.back().size() - lastFragmentFreeData;
    }

    // Resets the size of the payload to 0
    void Reset();

    // Returns the protocol of transport to which this payload belongs
    const String& GetProtocol() const { return _protocol;}

    // This method copies the contents of this payload into a contiguous destination.
    // BAD THINGS WILL HAPPEN if dst is not big enough to hold Size() bytes
    void CopyInto(Binary_t* dst);

private: // funcs

    // This helper method ensures that we have enough capacity to perform a write at a given offset, with the given length
    void _ensureCapacity(size_t offset, size_t length);

private: // fields
    // the fragments that span the payload. These will be automatically released back to their allocator when
    // this object is destroyed
    std::vector<Fragment> _fragments;

    // the offset of the byte after the last byte of this payload. This is the same as the offset at which
    // we will append new data. It is also the same as the total number of user bytes currently held
    size_t _appendOffset;

    // the total capacity of the payload
    size_t _totalCapacity;

    // the function we can use to allocate new fragments
    Allocator_t _allocator;

    // the type of payload
    String _protocol;

}; // class Payload
} // namespace k2tx
