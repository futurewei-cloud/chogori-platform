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
    ClientRequest
};

//
//  Endpoint identifies address of the Node or client. TODO: change to something more appropriate than 'String'.
//
typedef String Endpoint;

class PayloadReader;

//
//  Payload is abstraction representing message content. Underneath can reference several buffers.
//  Due to that using indexer can be not efficient. For efficient sequential reading uses PayloadReader (getReader).
//
class Payload
{
    friend class PayloadReader;
protected:
    std::vector<Binary> buffers;
    uint64_t totalSize;

    struct Position
    {
        uint32_t buffer;
        uint32_t offset;
    };

    Position navigate(size_t offset) const
    {        
        assert(offset > 0 && offset < totalSize);
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
    Payload(std::vector<Binary> buffers) : buffers(std::move(buffers))
    {
        for(Binary& buffer : buffers)
        {
            totalSize += buffer.size();
        }
    }

    Payload(const Payload&) = default;
    Payload& operator=(const Payload&) = default;
    Payload(Payload&&) = default;
    Payload& operator=(Payload&& other) = default;


    uint64_t size() { return totalSize; } const

    uint8_t getByte(size_t offset) const
    {        
        Position position = navigate(offset);
        return buffers[position.buffer][position.offset];
    }

    uint8_t operator[](size_t index) const
    {
        return getByte(index);
    }

    PayloadReader getReader(size_t offset = 0) const;

    bool isEmpty() const
    {
        return buffers.empty();
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
            size_t needToCopy = std::min(size, currentBufferRemaining);
            
            std::memcpy(data, buffer.get()+position.offset, needToCopy);
            if(size >= currentBufferRemaining)
            {
                position.buffer++;
                position.offset = 0;
                size -= needToCopy;
                data = (void*)((char*)data + needToCopy);
            }
            else
            {
                position.offset += needToCopy;
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
    bool read(T& value)
    {
        return read((void*)&value, sizeof(value));
    }
};


PayloadReader Payload::getReader(size_t offset) const
{
    return PayloadReader(*this, navigate(offset));
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
