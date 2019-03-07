#pragma once

#include <cstdint>
#include "../common/Common.h"

namespace k2
{

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
    Endpoint sender;
    seastar::temporary_buffer<uint8_t> payload;

public:
    const MessageType getMessageType() { return messageType; }
    const PartitionAssignmentId& getPartition() { return partition; }
    const Endpoint& getSender() { return sender; }
    const seastar::temporary_buffer<uint8_t>& getPayload() { return payload; }
};

};  //  namespace k2
