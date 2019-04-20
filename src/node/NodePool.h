#pragma once

#include "common/Status.h"
#include "Module.h"
#include "common/ModuleId.h"
#include <memory>
#include "ISchedulingPlatform.h"
#include <seastar/core/sharded.hh>

namespace k2
{

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
protected:
    class LockScope
    {
        std::atomic_bool& lock;
    public:
        LockScope(std::atomic_bool& lock) : lock(lock)
        {
            bool expected = false;
            while(!lock.compare_exchange_strong(expected, true)) { expected = false; }
        }

        ~LockScope() { lock = false; }
    };

protected:
    std::vector<NodeEndpointConfig> endpoints;
    std::unordered_map<CollectionId, CollectionPtr> collections;
    std::unordered_map<ModuleId, std::unique_ptr<IModule>> modules;
    std::atomic_bool collectionLock { false };

    Status _internalizeCollection(CollectionMetadata&& metadata, Collection*& ptr)
    {
        //
        //  We store collection metadata per Pool, so Nodes will share it.
        //  Thus we need to have a lock to synchronize access between Nodes.
        //  It's not crucial since it happens only during partition assignment
        //
        LockScope lockScope(collectionLock);

        auto colIt = collections.find(metadata.getId());
        if(colIt != collections.end())
        {
            ptr = colIt->second.get();
            return Status::Ok;
        }

        auto moduleIt = modules.find(metadata.getModuleId());
        if(moduleIt == modules.end())
            return Status::ModuleIsNotRegistered;

        CollectionPtr newCollection(std::move(metadata), *moduleIt->second);
        ModuleResponse moduleResponse = moduleIt->second->onNewCollection(*newCollection.get());   //  Minimal work should be done here to prevent deadlocks
        if(moduleResponse.type != ModuleResponse::Ok)
            return Status::ModuleRejectedCollection;

        ptr = newCollection.get();
        collections[newCollection.get()->getId()] = std::move(newCollection);

        return Status::Ok;
    }

    ISchedulingPlatform* schedulingPlatform = nullptr;

public:
    Status internalizeCollection(CollectionMetadata&& metadata, Collection*& ptr)
    {
        RET(_internalizeCollection(std::move(metadata), ptr));
    }

    size_t getNodesCount() const { return endpoints.size(); }

    const NodeEndpointConfig& getEndpoint(size_t endpointId) const
    {
        assert(endpointId < getNodesCount());
        return endpoints[endpointId];
    }

    ISchedulingPlatform& getScheduingPlatform() { return *schedulingPlatform; }
};

//
//  Represents a class containing state of K2 Node Pool.
//
class NodePool : public INodePool
{
public:
    Status registerModule(ModuleId moduleId, std::unique_ptr<IModule>&& module)
    {
        auto emplaceResult = modules.try_emplace(moduleId, std::move(module));
        return emplaceResult.second ? Status::Ok : LOG_ERROR(Status::ModuleWithSuchIdAlreadyRegistered);
    }

    Status registerNode(NodeEndpointConfig nodeConfig)
    {
        endpoints.push_back(nodeConfig);
        return Status::Ok;
    }

    void setScheduingPlatform(ISchedulingPlatform* platform)
    {
        assert(!schedulingPlatform);
        assert(platform);
        schedulingPlatform = platform;
    }
};

}   //  namespace k2
