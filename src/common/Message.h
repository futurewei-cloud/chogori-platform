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
//
class PartitionMessage : public Message
{
protected:
    MessageType messageType;
    PartitionAssignmentId partition;
public:
    PartitionMessage(MessageType messageType, PartitionAssignmentId partition, Endpoint&& sender, Payload&& payload) :
        Message(std::move(sender), std::move(payload)), messageType(messageType), partition(partition) { }

    const MessageType getMessageType() { return messageType; }
    const PartitionAssignmentId& getPartition() { return partition; }
    const Endpoint& getSender() { return sender; }
    Payload& getPayload() { return payload; }

    void releasePayload()
    {
        payload.clear();
    }
};


//
//  Represent message sink to respond back to client
//
class ClientConnection
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
    virtual ~ClientConnection() {}
};


//
//  Message targeted partition and received by transport
//
class PartitionRequest
{
public:
    std::unique_ptr<PartitionMessage> message;
    std::unique_ptr<ClientConnection> client;

    PartitionRequest(PartitionRequest&& other) = default;
    PartitionRequest& operator=(PartitionRequest&& other) = default;
};


//
//  Message sent in response to PartitionMessage
//
class ResponseMessage : public Message
{
public:
    Status status;
    uint32_t moduleCode;

    Status getStatus() const { return status; }
};


//
//  OffloadMessage
//
class AssignmentMessage
{
public:
    PartitionMetadata partitionMetadata;
    CollectionMetadata collectionMetadata;
    PartitionVersion partitionVersion;

    AssignmentMessage() {}

    K2_PAYLOAD_FIELDS(partitionMetadata, collectionMetadata, partitionVersion);

    std::unique_ptr<PartitionMessage> createMessage(Endpoint&& receiver)
    {
        Payload payload;
        payload.getWriter().write(*this);

        return std::make_unique<PartitionMessage>(MessageType::PartitionAssign, PartitionAssignmentId(partitionMetadata.getId(), partitionVersion),
            Endpoint(""), std::move(payload));
    }
};

};  //  namespace k2
