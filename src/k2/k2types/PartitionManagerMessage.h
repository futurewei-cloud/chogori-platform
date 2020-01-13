#pragma once

#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>
#include "PartitionMetadata.h"

namespace k2
{

template <typename MessageType, typename RequestType>
class MessageWithType {
   public:
    MessageType type;
    RequestType& request;

    MessageWithType(RequestType& request) : type(RequestType::getMessageType()), request(request) {}

    K2_PAYLOAD_FIELDS(type, request);
};

template <typename RequestType>
static auto makeMessageWithType(RequestType& request) { return MessageWithType<decltype(RequestType::getMessageType()), RequestType>(request); }

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

class NodeInfo
{
public:
    String tcpHostAndPort;
    std::vector<String> endpoints;

    DEFAULT_COPY_MOVE_INIT(NodeInfo)
    K2_PAYLOAD_FIELDS(tcpHostAndPort, endpoints);
};

class NodePoolRegistrationMessage
{
public:
    class Request
    {
    public:
        static constexpr MessageType getMessageType() { return MessageType::NodePoolRegistration; }

        String poolId;
        std::vector<NodeInfo> nodes;

        K2_PAYLOAD_FIELDS(poolId, nodes);
    };

    class Response
    {
    public:
        Status status = Status::S200_OK();
        long sessionId;
        std::vector<String> nodeIds;

        K2_PAYLOAD_FIELDS(sessionId, nodeIds);
    };
};

class HeartbeatMessage
{
public:
    class Request
    {
    public:
        static constexpr MessageType getMessageType() { return MessageType::Heartbeat; }

        String poolId;
        long sessionId; //  Value returned by Partition Manager after registration
        std::vector<String> nodeNames;

        K2_PAYLOAD_FIELDS(poolId, sessionId, nodeNames);
    };

    class Response
    {
    public:
        Status status = Status::S200_OK();
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

        PartitionVersion version { 0, 0 };
        String collection;

        K2_PAYLOAD_FIELDS(version, collection);
    };

    class Response
    {
    public:
        Status status = Status::S200_OK();
        CollectionId collectionId;
        PartitionMap partitionMap;
        std::map<String, std::vector<String>> shardEndpoints;    //  ShardId -> [endpoints]

        K2_PAYLOAD_FIELDS(collectionId, partitionMap, shardEndpoints);
    };
};


} // namespeace manager

}   //  namespace k2
