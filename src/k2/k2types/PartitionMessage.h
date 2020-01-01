#pragma once

#include "PartitionMetadata.h"
#include <cassert>
#include "Message.h"

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

        K2_PAYLOAD_COPYABLE;
    };

    template<class MessageT>
    static void serializeMessage(Payload& payload, MessageType messageType, PartitionAssignmentId partition, const MessageT& messageContent)
    {
        auto cursor = payload.getCurrentPosition();
        Header header{
            .messageType = messageType,
            .partition = partition,
            .messageSize = 1};
        payload.write(header);
        auto msgCursor = payload.getCurrentPosition();
        payload.write(messageContent);

        header.messageSize = payload.getCurrentPosition().offset - msgCursor.offset;
        payload.seek(cursor);
        payload.write(header);
    }
};


//
//  Message targeted partition and received by transport
//
class PartitionRequest
{
public:
    std::unique_ptr<PartitionMessage> message;
    std::unique_ptr<IClientConnection> client;

    PartitionRequest(std::unique_ptr<PartitionMessage> message, std::unique_ptr<IClientConnection> client)
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

        K2_PAYLOAD_COPYABLE;
    };

    Status status;
    uint32_t moduleCode;

    ResponseMessage(Endpoint&& remote, Payload&& payload, const Header& header) :
        Message(std::move(remote), std::move(payload)),
        status(header.status),
        moduleCode(header.moduleCode) {}

    Status getStatus() const { return status; }
    uint32_t getModuleCode() const { return moduleCode;}
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


    std::unique_ptr<PartitionMessage> createMessage(Endpoint&& receiver, Payload&& payload)
    {
        payload.write(*this);

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

    static std::unique_ptr<PartitionMessage> createMessage(Endpoint&& receiver, const PartitionAssignmentId& partitionId, Payload&& payload)
    {
        return std::make_unique<PartitionMessage>(MessageType::PartitionOffload, partitionId, std::move(receiver), std::move(payload));
    }
};

}  //  namespace k2
