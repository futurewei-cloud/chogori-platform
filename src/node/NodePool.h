#pragma once

#include "common/Status.h"
#include "Module.h"
#include "common/ModuleId.h"
#include <memory>
#include <atomic>
#include "ISchedulingPlatform.h"
#include <config/NodePoolConfig.h>

namespace k2
{

class Node;
class PoolMonitor;

//
//  Describe current node pool API accessible for tasks
//
class INodePool
{
    DISABLE_COPY_MOVE(INodePool)
protected:
    INodePool(k2_shared_ptr<NodePoolConfig> pConfig)
    : config(pConfig)
    {
        // empty
    }

    INodePool()
    : INodePool(k2_make_shared<NodePoolConfig>())
    {
        // empty
    }

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
    std::vector<Node*> nodes;   //  Use pointer instead of unique_ptr here just because, we don't want include Node.h in NodePool.h to cause cyclic dependency
    std::unordered_map<CollectionId, CollectionPtr> collections;
    std::unordered_map<ModuleId, std::unique_ptr<IModule>> modules;
    std::atomic_bool collectionLock { false };

    k2_shared_ptr<NodePoolConfig> config;

    ISchedulingPlatform* schedulingPlatform = nullptr;
    PoolMonitor* monitorPtr = nullptr;
    String name;

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

public:
    Status internalizeCollection(CollectionMetadata&& metadata, Collection*& ptr)
    {
        RET(_internalizeCollection(std::move(metadata), ptr));
    }

    size_t getNodesCount() const { return nodes.size(); }

    Node& getNode(size_t nodeId)
    {
        ASSERT(nodeId < getNodesCount());
        return *nodes[nodeId];
    }

    ISchedulingPlatform& getScheduingPlatform() { return *schedulingPlatform; }

    Node& getCurrentNode() { return getNode(getScheduingPlatform().getCurrentNodeId()); }

    const NodePoolConfig& getConfig() const { return *config; }

    const PoolMonitor& getMonitor() const { return *monitorPtr; }

    bool isTerminated() const { return false; }

    const String& getName() const { return name; }
};

}   //  namespace k2
