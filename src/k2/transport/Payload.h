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

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <limits>

#include <k2/common/Common.h>
#include <k2/common/Log.h>

namespace k2 {

// Serialize-helper class which allows the user to serialize a custom type as a Payload type.
// This allows any reader to read this field as a Payload, and deserialize a custom type from
// it at a later point. In particular, we use this to send/receive generic records in K2.
template <typename T>
struct SerializeAsPayload {
    T val;
};

//
//  Serialization traits
//
template <class T, class R = void>
struct enable_if_type { typedef R type; };

template <typename T, typename = void>
struct IsPayloadSerializableTypeTrait : std::false_type {};

template <typename T>
struct IsPayloadSerializableTypeTrait<T, typename enable_if_type<typename T::__K2PayloadSerializableTraitTag__>::type> : std::true_type {};

template <typename T, typename = void>
struct IsPayloadCopyableTypeTrait : std::false_type {};

template <typename T>
struct IsPayloadCopyableTypeTrait<T, typename enable_if_type<typename T::__K2PayloadCopyableTraitTag__>::type> : std::true_type {};

template <typename T>  //  Type that can be just copy to/from payload, though may not have reference
constexpr bool isNumericType() { return std::is_arithmetic<T>::value || std::is_enum<T>::value; }

template <typename T>  //  Type that can be just copy to/from payload (e.g. integers, enums, etc.)
constexpr bool isPayloadCopyableType() { return IsPayloadCopyableTypeTrait<T>::value; }

template <typename T>  //  Type that need custom serialization to convert to/from payload
constexpr bool isPayloadSerializableType() { return IsPayloadSerializableTypeTrait<T>::value; }

//  Payload is abstraction representing message content. It allows for very efficient network
// transportation of bytes, and it allows for allocating the underlying memory in a network-aware way.
// For that reason, normally payloads are produced by the k2 transport, either when a new message comes in
// or when the application wants to send a message.
// The Payload also has API to allow efficient (de)serialization
class Payload {
private: // types
    // payloads are made of small chunks of this size
    typedef uint32_t _Size;

public: // types
    // We use this class to represent a position/cursor in this payload.
    // Positions are never invalid but it is possible that they point to buffer not yet allocated
    struct PayloadPosition {
        PayloadPosition();
        PayloadPosition(_Size bIdx, _Size bOff, size_t offset);
        _Size bufferIndex;
        _Size bufferOffset;
        size_t offset;
    };

public: // Lifecycle
    // Create a blank payload which can grow by allocating with the given allocator
    Payload(BinaryAllocatorFunctor allocator);
    static Binary DefaultAllocator();

    Payload(Payload&&) = default;
    Payload& operator=(Payload&& other) = default;

public: // memory management
    bool isEmpty() const;

    // returns the total data size in this Payload
    size_t getSize() const;

    // returns the total memory allocated in this Payload
    size_t getCapacity() const;

    // makes sure that the payload has enough total capacity to hold the given totalCapacity
    void ensureCapacity(size_t totalCapacity);

    // release the underlying buffers
    std::vector<Binary> release();

    // Returns a ref-counted shared view of the payload. The new payload will have its own cursor and will
    // share the data which was present here at time of share.
    // - any new data appended to either payload will not be visible to any other payload
    // - any changes on the common data will be visible to both
    // the underlying data will not be destroyed until all shared payloads are destroyed.
    Payload share();

    // Same as shart(), but just share nbytes
    Payload share(size_t nbytes);

    // Creates a new payload as a copy of this payload. The underlying data is copied over to the new payload
    Payload copy();

    // clear this payload
    void clear();

public: // read-only API. Used to wrap an external list of buffers into a Payload
    // Wrap the given buffers into the Payload interface. No further allocation will be possible
    Payload(std::vector<Binary>&& externallyAllocatedBuffers, size_t containedDataSize);

    // Create an empty payload. Suitable for filling with external binaries via appendBinary()
    Payload();

    // Append a buffer to this Payload. Only possible for non-allocating payloads
    void appendBinary(Binary&& binary);

public: // API
    // sets the current R/W position of this payload to the given offset
    void seek(size_t offset);

    // seek to a position
    void seek(PayloadPosition position);

    // returns the current R/W position
    PayloadPosition getCurrentPosition() const;

    // returns the total bytes left for reading in this payload
    size_t getDataRemaining() const;

    // This method computes crc32c over the remaining data in the buffer
    uint32_t computeCrc32c();

    // compare with the given payload. linear in complexity of number of bytes stored in the payload
    bool operator==(const Payload& o) const;

public:  // Read API
    // Copy raw bytes from the payload into the given pointer. The pointer data must point to
    // location with allocated at least *size* bytes.
    // returns true if we were able to copy all of the requested bytes
    // if we cannot copy the requested bytes, the payload's position is left unmodified
    bool read(void* data, size_t size);

    // Read some bytes into the given binary
    bool read(Binary& binary, size_t size);

    // read a single character
    bool read(char& b);

    // read a string
    bool read(String& value);

    // read into a payload
    bool read(Payload& other);

    // read a duration value
    bool read(Duration& dur);

    template<typename T>
    bool read(SerializeAsPayload<T>& value) {
        // if the embedded type is a Payload, then just use the payload write to write it directly
        if (std::is_same<T, Payload>::value) {
            return read(value.val);
        }
        uint64_t size = 0;
        if (!read(size)) {
            return false;
        }
        if (size == 0) {
            return true;
        }

        return read(value.val);
    }

    // read a map
    template <typename KeyT, typename ValueT>
    bool read(std::map<KeyT, ValueT>& m) {
        _Size size;
        if (!read(size))
            return false;

        for (_Size i = 0; i < size; i++) {
            KeyT key;
            ValueT value;

            if (!read(key) || !read(value))
                return false;

            m[std::move(key)] = std::move(value);
        }

        return true;
    }

    // read an unordered_map
    template <typename KeyT, typename ValueT>
    bool read(std::unordered_map<KeyT, ValueT>& m) {
        _Size size;
        if (!read(size))
            return false;
        m.reserve(size);

        for (_Size i = 0; i < size; i++) {
            KeyT key;
            ValueT value;

            if (!read(key) || !read(value))
                return false;

            m[std::move(key)] = std::move(value);
        }

        return true;
    }

    // read a vector
    template <typename ValueT>
    bool read(std::vector<ValueT>& vec) {
        _Size size;
        if (!read(size))
            return false;
        vec.reserve(size);

        for (_Size i = 0; i < size; i++) {
            ValueT value;
            if (!read(value))
                return false;

            vec.push_back(std::move(value));
        }

        return true;
    }

    // read a set
    template <typename T>
    bool read(std::set<T>& s) {
        _Size size;
        if (!read(size))
            return false;

        for (_Size i = 0; i < size; i++) {
            T key;

            if (!read(key))
                return false;

            s.insert(std::move(key));
        }

        return true;
    }

    // read an unordered set
    template <typename T>
    bool read(std::unordered_set<T>& s) {
        _Size size;
        if (!read(size))
            return false;
        s.reserve(size);

        for (_Size i = 0; i < size; i++) {
            T key;

            if (!read(key))
                return false;

            s.insert(std::move(key));
        }

        return true;
    }

    // primitive type read
    template <typename T>
    std::enable_if_t<isPayloadCopyableType<T>() || isNumericType<T>(), bool> read(T& value) {
        return read((void*)&value, sizeof(value));
    }

    // serializable type read
    template <typename T>
    std::enable_if_t<isPayloadSerializableType<T>(), bool> read(T& value) {
        return value.__readFields(*this);
    }

    // read many values in series
    template <typename T, typename... ArgsT>
    bool readMany(T& value, ArgsT&... args) {
        return read(value) && readMany(args...);
    }

    // no-arg version to satisfy the template expansion above in the terminal case
    bool readMany();

public: // Write API
    // copy size bytes from the given payload into this payload
    bool copyFromPayload(Payload& src, size_t toCopy);

    // this method skips the given number of bytes as if a write of that size occurred
    void skip(size_t advance);

    // Truncates this payload to the current cursor position, dropping the remaining data
    void truncateToCurrent();

    // write a single character
    void write(char b);

    // copy size bytes from the memory pointed to by data
    void write(const void* data, size_t size);

    // write a string
    void write(const String& value);

    // write another Payload
    void write(const Payload& other);

    // Write a duration value
    void write(const Duration& dur);

    // write a map
    template <typename KeyT, typename ValueT>
    void write(const std::map<KeyT, ValueT>& m) {
        K2ASSERT(m.size() < std::numeric_limits<_Size>::max(), "map is too long to write out");
        write((_Size)m.size());

        for (auto& kvp : m) {
            write(kvp.first);
            write(kvp.second);
        }
    }

    // write an unordered_map
    template <typename KeyT, typename ValueT>
    void write(const std::unordered_map<KeyT, ValueT>& m) {
        K2ASSERT(m.size() < std::numeric_limits<_Size>::max(), "map is too long to write out");
        write((_Size)m.size());

        for (auto& kvp : m) {
            write(kvp.first);
            write(kvp.second);
        }
    }
    // write a vector
    template <typename ValueT>
    void write(const std::vector<ValueT>& vec) {
        K2ASSERT(vec.size() < std::numeric_limits<_Size>::max(), "vector is too long to write out");
        write((_Size)vec.size());

        for (const ValueT& value : vec) {
            write(value);
        }
    }

    // write a set
    template <typename T>
    void write(const std::set<T>& s) {
        K2ASSERT(s.size() < std::numeric_limits<_Size>::max(), "set is too long to write out");
        write((_Size)s.size());

        for (auto& key : s) {
            write(key);
        }
    }

    // write a set
    template <typename T>
    void write(const std::unordered_set<T>& s) {
        K2ASSERT(s.size() < std::numeric_limits<_Size>::max(), "set is too long to write out");
        write((_Size)s.size());

        for (auto& key : s) {
            write(key);
        }
    }

    // write the special SerializeAsPayload type
    template<typename T>
    void write(const SerializeAsPayload<T>& value) {
        // if the embedded type is a Payload, then just use the payload write to write it directly
        if (std::is_same<T, Payload>::value || std::is_same<T, const Payload>::value) {
            write(value.val);
            return;
        }
        // 1. write out a dummy size now
        auto sizePos = getCurrentPosition();
        uint64_t size = 0;
        write(size);

        // 2. write the actual value
        auto valPos = getCurrentPosition();
        write(value.val);

        // 3. calculate how much data we wrote and update the size we wrote in step1
        auto nowPos = getCurrentPosition();
        size = nowPos.offset - valPos.offset;
        seek(sizePos);
        write(size);
        // make sure to place the cursor at end of all the written data
        seek(nowPos);
    }

    // write for primitive types by copy
    template <typename T>
    std::enable_if_t<isNumericType<T>(), void> write(const T value) {
        write((const void*)&value, sizeof(value));
    }

    // write for copyable types
    template <typename T>
    std::enable_if_t<isPayloadCopyableType<T>(), void> write(const T& value) {
        write((const void*)&value, sizeof(value));
    }

    // write for serializable types
    template <typename T>
    std::enable_if_t<isPayloadSerializableType<T>(), void> write(const T& value) {
        value.__writeFields(*this);
    }

    // write out many fields at once
    template <typename T, typename... ArgsT>
    void writeMany(T& value, ArgsT&... args) {
        write(value);
        writeMany(args...);
    }

    // no-arg version to satisfy the template expansion above in the terminal case
    void writeMany();

private:  // types and fields

    std::vector<Binary> _buffers;
    size_t _size; // total bytes of user data present
    size_t _capacity; // total bytes allocated in the buffers.
    BinaryAllocatorFunctor _allocator;
    PayloadPosition _currentPosition;

private: // helper methods
    // used to allocate additional space
    bool _allocateBuffer();

    // advances the current position by the given number
    void _advancePosition(size_t advance);

private: // deleted
    Payload(const Payload&) = delete;
    Payload& operator=(const Payload&) = delete;
}; // class Payload

} //  namespace k2
