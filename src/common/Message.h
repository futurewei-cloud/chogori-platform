#pragma once

#include "PartitionMetadata.h"
#include <cassert>

namespace k2
{

//
//  Describe the type of K2 message
//
enum class MessageType : uint8_t
{
    None = 0,   //  Error
    PartitionAssign,
    PartitionOffload,
    ClientRequest,
    Response,
};

//
//  Endpoint identifies address of the Node or client. TODO: change to something more appropriate than 'String'.
//
typedef String Endpoint;

class PayloadReader;
class PayloadWriter;

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
        assert(offset >= 0 && offset < size);
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
public:    
    Payload() : size(0) { }
    Payload(std::vector<Binary> buffers, size_t size) : buffers(std::move(buffers)), size(size) { }
    Payload(const Payload&) = delete;
    Payload& operator=(const Payload&) = delete;
    Payload(Payload&&) = default;
    Payload& operator=(Payload&& other) = default;

    size_t getSize() { return size; } const

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
};

#define K2_PAYLOAD_FIELDS(...)                      \
        struct __K2PayloadSerializableTraitTag__ {};\
        bool writeFields(PayloadWrite& write)       \
        {                                           \
            return writer.write(__VA_ARGS__);       \
        }                                           \
        bool readFields(PayloadReader& reader)      \
        {                                           \
            return reader.read(__VA_ARGS__);        \
        }                                           \

template<typename T, typename = void>
struct IsPayloadTypeTrait : std::false_type { };

template<typename T>
struct IsPayloadTypeTrait<T, typename T::__K2PayloadSerializableTraitTag__> : std::true_type { };

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

    constexpr bool read() { return true; }

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

    bool read(uint8_t b)
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

    template<typename T>
    typename std::enable_if<!IsPayloadTypeTrait<T>::value, bool>::type read(T& value) //  TODO: need to make it less ambiguous
    {
        return read((void*)&value, sizeof(value));
    }

    template<typename T>
    typename std::enable_if<IsPayloadTypeTrait<T>::value, bool>::type read(T& value) //  TODO: need to make it less ambiguous
    {
        return value.readFields(*this);
    }

    template<typename T, typename... ArgsT>
    bool read(T& value, ArgsT... args)
    {
        if(!read(value))
            return false;

        if(!read(args...))
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
        payload.buffers.push_back(std::move(Binary(8096))); //  TODO: in DPDK case allocate from NIC buffer pool
        return true;
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
        return write((uint32_t)value.size()) && write(value.data(), value.size());
    }

    template<typename T>
    bool write(T& value)  //  TODO: need to make it less ambiguous
    {
        return write((void*)&value, sizeof(value));
    }

    template<typename T, typename... ArgsT>
    bool write(T& value, ArgsT... args)
    {
        if(!write(value))
            return false;

        if(!write(args...))
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
    return PayloadWriter(*this, offset);
}

//
//  Class represents message obtained from transport
//
class Message
{
protected:
    MessageType messageType;
    PartitionAssignmentId partition;    
    Payload payload;
    Endpoint sender;
    
public:
    const MessageType getMessageType() { return messageType; }
    const PartitionAssignmentId& getPartition() { return partition; }
    const Endpoint& getSender() { return sender; }
    const Payload& getPayload() { return payload; }
};

//
//  
//
class ClientConnection
{
    Endpoint sender;
public:

    //
    //  Send error to sender
    //
    void errorResponse(Status status) {}    //  TODO

    //
    //  Send reponse to sender
    //
    void messageResponse(std::unique_ptr<Message> message) {}    //  TODO

    //
    //  Return address of the sender
    //
    const Endpoint& getSender() { return sender; }
};


//
//  Message received by transport
//
class ReceivedMessage : public Message
{
public:
    std::unique_ptr<ClientConnection> client;
};


//
//  OffloadMessage
//
class AssignMessage
{
public:    
    std::unique_ptr<PartitionMetadata> partitionMetadata;
    std::unique_ptr<CollectionMetadata> collectionMetadata;

    AssignMessage() {}

    bool parse(const Payload& payload)
    {
        return true;
    }

    static Binary format(const PartitionMetadata& partitionMetadata, const CollectionMetadata& collectionMetadata)
    {
        return Binary();    //  TODO: format
    }
};

};  //  namespace k2
