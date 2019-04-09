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
//  Message traveled through the system
//
class Message
{
protected:
    Endpoint sender;
public:
    Payload payload;

    Message() {}
    Message(Endpoint&& sender, Payload&& payload) : sender(std::move(sender)), payload(std::move(payload)) { }
};

//
//  Class represents message obtained from transport
//  TODO: need to refactore class because currently is not quite clear whether payload contains message header (it doesn't,
//  but builder will generate Payload with header. Thus this payload cannot be used from PartitionMessage).
//  This is something that needs to be more explicitly presented in class hierarchy.
//
class PartitionMessage : public Message
{
protected:
    MessageType messageType;
    PartitionAssignmentId partition;
public:
    PartitionMessage(MessageType messageType, PartitionAssignmentId partition, Endpoint&& sender, Payload&& payload) :
        Message(std::move(sender), std::move(payload)), messageType(messageType), partition(partition) { }

    MessageType getMessageType() { return messageType; }
    const PartitionAssignmentId& getPartition() { return partition; }
    const Endpoint& getSender() { return sender; }
    Payload& getPayload() { return payload; }

    void releasePayload()
    {
        payload.clear();
    }

    struct Header
    {
        MessageType messageType;
        PartitionAssignmentId partition;
        size_t messageSize;

        K2_PAYLOAD_COPYABLE
    };

    class PayloadBuilder
    {
    protected:
        Payload payload;
        Header* header;
    public:
        PayloadBuilder(MessageType messageType, PartitionAssignmentId partition)
        {
            assert(payload.getWriter().getContiguousStructure(header));
            header->messageType = messageType;
            header->partition = partition;
        }

        PayloadWriter getWriter()
        {
            assert(header);
            return payload.getWriter(sizeof(Header));
        }

        Payload&& done()
        {
            assert(header);
            header->messageSize = payload.getSize()-sizeof(Header);
            header = nullptr;
            return std::move(payload);
        }
    };

    template<class MessageT>
    static Payload serializeMessage(MessageType messageType, PartitionAssignmentId partition, const MessageT& messageContent)
    {
        PayloadBuilder builder(messageType, partition);
        builder.getWriter().write(messageContent);
        return std::move(builder.done());
    }
};


//
//  Represent message sink to respond back to client
//
class IClientConnection
{
protected:
    Endpoint sender;
public:
    //
    //  Send reponse to sender
    //
    virtual PayloadWriter getResponseWriter() = 0;

    //
    //  Send error to sender
    //
    virtual void sendResponse(Status status, uint32_t code = 0) = 0;

    //
    //  Return address of the sender
    //
    const Endpoint& getSender() { return sender; }

    //
    //  Destructor
    //
    virtual ~IClientConnection() {}
};


//
//  Message targeted partition and received by transport
//
class PartitionRequest
{
public:
    std::unique_ptr<PartitionMessage> message;
    std::unique_ptr<IClientConnection> client;

    PartitionRequest(std::unique_ptr<PartitionMessage>&& message, std::unique_ptr<IClientConnection>&& client)
        : message(std::move(message)), client(std::move(client)) {}

    PartitionRequest() {}

    PartitionRequest(PartitionRequest&& other) = default;
    PartitionRequest& operator=(PartitionRequest&& other) = default;
};


//
//  Message sent in response to PartitionMessage
//
class ResponseMessage : public Message
{
public:
    struct Header
    {
        Status status;
        uint32_t moduleCode;
        size_t messageSize;
    };

    Status status;
    uint32_t moduleCode;

    ResponseMessage() {}
    ResponseMessage(const Header& header) : status(header.status), moduleCode(header.moduleCode) {}

    Status getStatus() const { return status; }
};


//
//  Message sent with partition assignment command
//
class AssignmentMessage
{
public:
    PartitionMetadata partitionMetadata;
    CollectionMetadata collectionMetadata;
    PartitionVersion partitionVersion;

    PartitionAssignmentId getPartitionAssignmentId() const
    {
        return PartitionAssignmentId(partitionMetadata.getId(), partitionVersion);
    }

    AssignmentMessage() {}

    std::unique_ptr<PartitionMessage> createMessage(Endpoint&& receiver)
    {
        Payload payload;
        payload.getWriter().write(*this);

        return std::make_unique<PartitionMessage>(MessageType::PartitionAssign, getPartitionAssignmentId(),
            std::move(receiver), std::move(payload));
    }

    K2_PAYLOAD_FIELDS(partitionMetadata, collectionMetadata, partitionVersion);
};

//
//  Offload message
//
class OffloadMessage
{
public:
    OffloadMessage() {}

    static std::unique_ptr<PartitionMessage> createMessage(Endpoint&& receiver, const PartitionAssignmentId& partitionId)
    {
        return std::make_unique<PartitionMessage>(MessageType::PartitionOffload, partitionId, std::move(receiver), Payload());
    }
};

}  //  namespace k2
