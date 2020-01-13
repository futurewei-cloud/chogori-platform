#pragma once

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <limits>

#include <k2/common/Common.h>
#include <k2/common/Log.h>

namespace k2 {

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

public: // Read API
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

        for (_Size i = 0; i < size; i++) {
            ValueT value;
            if (!read(value))  //  TODO: read directly into array
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
