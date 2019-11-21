#pragma once

#include "common/Status.h"
#include "Module.h"
#include "common/ModuleId.h"
#include <memory>
#include <atomic>
#include "ISchedulingPlatform.h"
#include "NodeConfig.h"

namespace k2
{

class Node;
class PoolMonitor;

//
//  Address configuration for endpoint
//
struct NodeEndpointConfig
{
    enum EndpointType
    {
        Ethernet,    //  MAC address specified
        IPv4,   //  IPv4 and port
        IPv6,   //  IPv6 and port
    };

    struct EthernetEndpoint
    {
        __extension__ unsigned __int128 address;
    };

    struct IPv4Endpoint
    {
        uint32_t address;
        uint16_t port;
    };

    struct IPv6Endpoint
    {
        __extension__ unsigned __int128 address;
        uint16_t port;
    };

    EndpointType type;

    union
    {
        EthernetEndpoint ethernet;
        IPv4Endpoint ipv4;
        IPv6Endpoint ipv6;
    };
};

//
//  Describe current node pool API accessible for tasks
//
class INodePool
{
    DISABLE_COPY_MOVE(INodePool)
protected:
    INodePool();

    class LockScope
    {
        std::atomic_bool& lock;
    public:
        LockScope(std::atomic_bool& lock);

        ~LockScope();
    };

protected:
    std::vector<Node*> nodes;   //  Use pointer instead of unique_ptr here just because, we don't want include Node.h in NodePool.h to cause cyclic dependency
    std::unordered_map<CollectionId, CollectionPtr> collections;
    std::unordered_map<ModuleId, std::unique_ptr<IModule>> modules;
    std::atomic_bool collectionLock { false };

    NodePoolConfig config;

    ISchedulingPlatform* schedulingPlatform = nullptr;
    PoolMonitor* monitorPtr = nullptr;
    String name;

    Status _internalizeCollection(CollectionMetadata&& metadata, Collection*& ptr);

public:
    Status internalizeCollection(CollectionMetadata&& metadata, Collection*& ptr);

    size_t getNodesCount() const;

    Node& getNode(size_t nodeId);

    ISchedulingPlatform& getScheduingPlatform();

    Node& getCurrentNode();

    const NodePoolConfig& getConfig() const;

    const PoolMonitor& getMonitor() const;

    bool isTerminated() const;

    const String& getName() const;
};
}   //  namespace k2
