#pragma once

#include <cassert>
#include "Common.h"
#include <seastar/net/packet.hh>
#include <boost/asio/buffer.hpp>

namespace k2
{

class PayloadReader;
class PayloadWriter;

//
//  Serialization traits
//
template<class T, class R = void>
struct enable_if_type { typedef R type; };

template<typename T, typename = void>
struct IsPayloadSerializableTypeTrait : std::false_type { };

template<typename T>
struct IsPayloadSerializableTypeTrait<T, typename enable_if_type<typename T::__K2PayloadSerializableTraitTag__>::type> : std::true_type { };

template<typename T, typename = void>
struct IsPayloadCopyableTypeTrait : std::false_type { };

template<typename T>
struct IsPayloadCopyableTypeTrait<T, typename enable_if_type<typename T::__K2PayloadCopyableTraitTag__>::type> : std::true_type { };

template<typename T> //  Type that can be just copy to/from payload, though may not have reference
constexpr bool isNumbericType() { return std::is_arithmetic<T>::value || std::is_enum<T>::value; }

template<typename T> //  Type that can be just copy to/from payload (e.g. integers, enums, etc.)
constexpr bool isPayloadCopyableType() { return IsPayloadCopyableTypeTrait<T>::value; }

template<typename T> //  Type that need custom serialization to convert to/from payload
constexpr bool isPayloadSerializableType() { return IsPayloadSerializableTypeTrait<T>::value; }

//
//  Payload is abstraction representing message content. Underneath can reference several buffers.
//  Due to that using indexer can be not efficient. For efficient sequential reading uses PayloadReader (getReader).
//
class Payload
{
    friend class PayloadReader;
    friend class PayloadWriter;
protected:
    std::vector<Binary> buffers;
    size_t size;

    struct Position
    {
        uint32_t buffer;
        uint32_t offset;
    };

    Position navigate(size_t offset) const
    {
        assert(offset <= size);
        if(offset == 0)
            return Position { 0, 0 };
        size_t bufferOffset = 0;
        for(uint32_t i = 0; i < buffers.size(); i++)    //  TODO: use binary search on separate array
        {
            size_t endBufferOffset = bufferOffset + buffers[i].size();
            if(endBufferOffset > offset)
                return Position { i, (uint32_t)(offset - bufferOffset) };
            bufferOffset = endBufferOffset;
        }
        assert(false);
        return Position {};
    }

    bool allocateBuffer()
    {
        buffers.push_back(std::move(Binary(8096))); //  TODO: in DPDK case allocate from NIC buffer pool
        return true;
    }

public:
    Payload() : size(0) { }
    Payload(std::vector<Binary> buffers, size_t size) : buffers(std::move(buffers)), size(size) { }
    Payload(const Payload&) = delete;
    Payload& operator=(const Payload&) = delete;
    Payload(Payload&&) = default;
    Payload& operator=(Payload&& other) = default;

    size_t getSize() const { return size; }

    size_t getAllocationSize() const
    {
        size_t totalSize = 0;
        for(const Binary& buffer : buffers)
            totalSize += buffer.size();
        return totalSize;
    }

    uint8_t getByte(size_t offset) const
    {
        Position position = navigate(offset);
        return buffers[position.buffer][position.offset];
    }

    uint8_t operator[](size_t index) const
    {
        return getByte(index);
    }

    bool isEmpty() const
    {
        return buffers.empty();
    }

    PayloadReader getReader(size_t offset = 0) const;

    PayloadWriter getWriter(size_t offset = 0);

    void clear()
    {
        buffers.clear();
        size = 0;
    }

    std::vector<boost::asio::const_buffer> toBoostBuffers() const
    {
        std::vector<boost::asio::const_buffer> result(buffers.size());
        for(auto& buffer : buffers)
            result.emplace_back(buffer.get(), buffer.size());

        return result;
    }

    static seastar::net::packet toPacket(Payload&& payload)
    {
        seastar::net::packet result;
        for(Binary& data : payload.buffers)
            result = seastar::net::packet(std::move(result), moveCharTempBuffer(data));

        payload.clear();

        return result;
    }
};


//
//  PayloadReader helps to navigate through the payload and read the message. Create by the Payload
//
class PayloadReader
{
    friend class Payload;
protected:
    const Payload& payload;
    Payload::Position position;
    PayloadReader(const Payload& payload, Payload::Position position) : payload(payload), position(position) { }

    constexpr bool readMany() { return true; }

public:
    bool isEnd() const
    {
        return position.buffer == payload.buffers.size();
    }

    bool read(void* data, size_t size)
    {
        while(size > 0)
        {
            if(isEnd())
                return false;

            const Binary& buffer = payload.buffers[position.buffer];
            size_t currentBufferRemaining = buffer.size()-position.offset;
            size_t needToCopySize = std::min(size, currentBufferRemaining);

            std::memcpy(data, buffer.get()+position.offset, needToCopySize);
            if(size >= currentBufferRemaining)
            {
                position.buffer++;
                position.offset = 0;
                size -= needToCopySize;
                data = (void*)((char*)data + needToCopySize);
            }
            else
            {
                position.offset += needToCopySize;
                break;
            }
        }

        return true;
    }

    bool read(Binary& binary, size_t size)
    {
        if(isEnd())
            return false;

        Binary& buffer = const_cast<Binary&>(payload.buffers[position.buffer]);
        size_t currentBufferRemaining = buffer.size()-position.offset;
        if(currentBufferRemaining >= size)  //  Can reference buffer
        {
            binary = buffer.share(position.offset, size);
            if(currentBufferRemaining == size)
                position.buffer++;
            else
                position.offset += size;

            return true;
        }

        //  Need to copy
        binary = Binary(size);  //  TODO: probably need to check size condition before trying to allocate
        return read(binary.get_write(), size);
    }

    bool read(uint8_t& b)
    {
        if(isEnd())
            return false;

        const Binary& buffer = payload.buffers[position.buffer];
        b = buffer[position.offset];
        if(position.offset == buffer.size() - 1)
        {
            position.offset = 0;
            position.buffer++;
        }
        else
            position.offset++;

        return true;
    }

    bool read(String& value)
    {
        uint32_t size;
        if(!read(size))
            return false;

        value.resize(size);

        return read((void*)value.data(), size);
    }

    template<typename KeyT, typename ValueT>
    bool read(std::map<KeyT, ValueT>& m)
    {
        m.clear();

        uint32_t size;
        if(!read(size))
            return false;

        for(uint32_t i = 0; i < size; i++)
        {
            KeyT key;
            ValueT value;

            if(!read(key) || !read(value))
                return false;

            m[std::move(key)] = std::move(value);
        }

        return true;
    }

    template<typename T>    //  Read for primitive types
    std::enable_if_t<isPayloadCopyableType<T>() || isNumbericType<T>(), bool> read(T& value)
    {
        return read((void*)&value, sizeof(value));
    }

    template<typename T>
    std::enable_if_t<isPayloadSerializableType<T>(), bool> read(T& value)
    {
        return value.readFields(*this);
    }

    template<typename T, typename... ArgsT>
    bool readMany(T& value, ArgsT&... args)
    {
        if(!read(value))
            return false;

        if(!readMany(args...))
            return false;

        return true;
    }
};


//
//  Helper class to build payload.
//  TODO: we may want to give users buffer awareness, so they can fit their object within single buffer.
//  In this case, even buffers in the middle of payload may have different lengths.
//
class PayloadWriter
{
    friend class Payload;
protected:
    Payload& payload;
    Payload::Position position;
    size_t offset;

    bool allocateBuffer()
    {
        return payload.allocateBuffer();
    }

    bool allocateBufferIfNeeded()
    {
        return isAllocationNeeded() ? allocateBuffer() : true;
    }

    void moveToNextBufferIfNeeded()
    {
        if(position.buffer == payload.buffers.size()-1 && payload.buffers[position.buffer].size() == position.offset)
        {
            position.buffer++;
            position.offset = 0;
        }
    }

    bool isAllocationNeeded()
    {
        moveToNextBufferIfNeeded();
        return position.buffer == payload.buffers.size();
    }

    void increaseGlobalOffset(size_t change)
    {
        offset += change;
        if(payload.size < offset)
            payload.size = offset;
    }

    bool writeMany() { return true; }

public:
    //
    //  Define writer position just in case we want to have some special data for writer
    //
    class Position
    {
        friend class PayloadWriter;
    protected:
        Payload::Position position;
        size_t offset;

        Position(Payload::Position position, size_t offset) : position(position), offset(offset) { }
    };

    PayloadWriter(Payload& payload, size_t offset) : payload(payload), offset(offset)
    {
        position = payload.navigate(offset);
    }

    PayloadWriter(Payload& payload, const PayloadWriter::Position& writerPosition) : payload(payload), position(writerPosition.position), offset(writerPosition.offset) {}

    bool write(uint8_t b)
    {
        if(!allocateBufferIfNeeded())
            return false;

        payload.buffers[position.buffer].get_write()[position.offset] = b;
        position.offset++;

        increaseGlobalOffset(1);

        return true;
    }

    bool write(const void* data, size_t size)
    {
        //  TODO: refactor to use iteration lambda
        if(!allocateBufferIfNeeded())
            return false;

        while(size > 0)
        {
            Binary& buffer = const_cast<Binary&>(payload.buffers[position.buffer]);

            size_t currentBufferRemaining = buffer.size()-position.offset;
            size_t needToCopySize = std::min(size, currentBufferRemaining);

            std::memcpy(buffer.get_write()+position.offset, data, needToCopySize);

            increaseGlobalOffset(needToCopySize);

            if(size >= currentBufferRemaining)
            {
                position.buffer++;
                position.offset = 0;
                size -= needToCopySize;
                data = (void*)((char*)data + needToCopySize);

                if(position.buffer == payload.buffers.size())
                {
                    if(!allocateBuffer())
                        return false;
                }
            }
            else
            {
                position.offset += needToCopySize;
                break;
            }
        }

        return true;
    }

    void truncateToCurrent()
    {
        if(position.buffer < payload.buffers.size()-1)
            payload.buffers.erase(payload.buffers.begin() + position.buffer + 1, payload.buffers.end());

        payload.size = offset;
    }

    Position getCurrent() const
    {
        return Position { position, offset };
    }

    template<typename StructT>
    bool getContiguousStructure(StructT*& structure)
    {
        return getContiguousBuffer(sizeof(StructT), *(void**)&structure);
    }

    bool getContiguousBuffer(size_t size, void*& data)
    {
        if(!allocateBufferIfNeeded())
            return false;

        Binary& buffer = const_cast<Binary&>(payload.buffers[position.buffer]);

        size_t currentBufferRemaining = buffer.size()-position.offset;
        if(currentBufferRemaining < size)
            return false;

        data = payload.buffers[position.buffer].get_write() + position.offset;
        if(currentBufferRemaining == size)
        {
            position.buffer++;
            position.offset = 0;
        }
        else
            position.offset += size;

        increaseGlobalOffset(size);

        return true;
    }

    bool skip(size_t size)
    {
        if(!allocateBufferIfNeeded())
            return false;

        while(size > 0)
        {
            Binary& buffer = const_cast<Binary&>(payload.buffers[position.buffer]);

            size_t currentBufferRemaining = buffer.size()-position.offset;
            size_t needToCopySize = std::min(size, currentBufferRemaining);

            increaseGlobalOffset(needToCopySize);

            if(size >= currentBufferRemaining)
            {
                position.buffer++;
                position.offset = 0;
                size -= needToCopySize;
                if(position.buffer == payload.buffers.size())
                {
                    if(!allocateBuffer())
                        return false;
                }
            }
            else
            {
                position.offset += needToCopySize;
                break;
            }
        }

        return true;
    }

    bool write(const String& value)  //  TODO: need to make it less ambiguous
    {
        uint32_t size = value.size();
        return write(size) && write(value.data(), value.size());
    }

    template<typename KeyT, typename ValueT>
    bool write(const std::map<KeyT, ValueT>& m)
    {
        if(!write((uint32_t)m.size()))
            return false;

        for(auto& kvp : m)
        {
            if(!write(kvp.first) || !write(kvp.second))
                return false;
        }

        return true;
    }

    template<typename T>    //  Read for primitive types - need copy here to get address
    std::enable_if_t<isNumbericType<T>(), bool> write(const T value)
    {
        return write((const void*)&value, sizeof(value));
    }

    template<typename T>    //  Read for primitive types
    std::enable_if_t<isPayloadCopyableType<T>(), bool> write(const T& value)
    {
        return write((const void*)&value, sizeof(value));
    }

    template<typename T>
    std::enable_if_t<isPayloadSerializableType<T>(), bool> write(const T& value)
    {
        return value.writeFields(*this);
    }

    template<typename T, typename... ArgsT>
    bool writeMany(T& value, ArgsT&... args)
    {
        if(!write(value))
            return false;

        if(!writeMany(args...))
            return false;

        return true;
    }
};

inline PayloadReader Payload::getReader(size_t offset) const
{
    return PayloadReader(*this, navigate(offset));
}

inline PayloadWriter Payload::getWriter(size_t offset)
{
    if(buffers.size() == 0)
        allocateBuffer();

    return PayloadWriter(*this, offset);
}

} //  namespace k2
