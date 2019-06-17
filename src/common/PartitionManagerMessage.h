#pragma once

#include "Serialization.h"

namespace k2
{

namespace manager
{

enum class MessageType : uint8_t
{
    Node = 0,
    NodePoolRegistration,
    Heartbeat,
    PartitionMapRequest,
};

template<typename RequestType>
using RequestMessage = MessageWithType<MessageType, RequestType>;

class PoolInfo
{
public:
    String nodePoolId;
    std::vector<String> nodeEndpoints;

    PoolInfo() {}

    DEFAULT_COPY_MOVE(PoolInfo)

    K2_PAYLOAD_FIELDS(nodePoolId, nodeEndpoints);
};

class NodePoolRegistrationMessage
{
public:
    class Request
    {
    public:
        static constexpr MessageType getMessageType() { return MessageType::NodePoolRegistration; }

        PoolInfo poolInfo;

        K2_PAYLOAD_FIELDS(poolInfo);
    };

    class Response
    {
    public:
        long sessionId;

        K2_PAYLOAD_FIELDS(sessionId);
    };
};

class HeartbeatMessage
{
public:
    class Request
    {
    public:
        static constexpr MessageType getMessageType() { return MessageType::Heartbeat; }

        PoolInfo poolInfo;
        long sessionId; //  Value returned by Partition Manager after registration

        K2_PAYLOAD_FIELDS(poolInfo, sessionId);
    };

    class Response
    {
    public:
        long sessionId;

        K2_PAYLOAD_FIELDS(sessionId);
    };
};

class PartitionMapMessage
{
public:
    class Request
    {
    public:
        static constexpr MessageType getMessageType() { return MessageType::PartitionMapRequest; }

        PartitionVersion version;   //  Lowest version to load

        K2_PAYLOAD_FIELDS(version);
    };
};


} // namespeace manager

}   //  namespace k2
