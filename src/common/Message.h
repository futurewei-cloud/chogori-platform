#pragma once

#include "PartitionMetadata.h"

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

//
//  Class represents message obtained from transport
//
class Message
{
protected:
    MessageType messageType;
    PartitionAssignmentId partition;    
    seastar::temporary_buffer<uint8_t> payload;
    Endpoint sender;
    
public:
    const MessageType getMessageType() { return messageType; }
    const PartitionAssignmentId& getPartition() { return partition; }
    const Endpoint& getSender() { return sender; }
    const Binary& getPayload() { return payload; }
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

    bool parse(const Binary& payload)
    {
        return true;
    }

    static Binary format(const PartitionMetadata& partitionMetadata, const CollectionMetadata& collectionMetadata)
    {
        return Binary();    //  TODO: format
    }
};

};  //  namespace k2
