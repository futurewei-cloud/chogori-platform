#include "NodePool.h"
namespace k2
{
INodePool::INodePool() {}

INodePool::LockScope::LockScope(std::atomic_bool& lock) : lock(lock)
{
    bool expected = false;
    while(!lock.compare_exchange_strong(expected, true)) { expected = false; }
}

INodePool::LockScope::~LockScope() { lock = false; }

Status INodePool::_internalizeCollection(CollectionMetadata&& metadata, Collection*& ptr)
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

Status INodePool::internalizeCollection(CollectionMetadata&& metadata, Collection*& ptr)
{
    return _internalizeCollection(std::move(metadata), ptr);
}

size_t INodePool::getNodesCount() const { return nodes.size(); }

Node& INodePool::getNode(size_t nodeId)
{
    assert(nodeId < getNodesCount());
    return *nodes[nodeId];
}

ISchedulingPlatform& INodePool::getSchedulingPlatform() { return *schedulingPlatform; }

Node& INodePool::getCurrentNode() { return getNode(getSchedulingPlatform().getCurrentNodeId()); }

const NodePoolConfig& INodePool::getConfig() const { return config; }

const PoolMonitor& INodePool::getMonitor() const { return *monitorPtr; }

bool INodePool::isTerminated() const { return false; }

const String& INodePool::getName() const { return name; }

}   //  namespace k2
