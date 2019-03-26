#pragma once

#include "../common/ModuleId.h"
#include "Module.h"
#include <memory>

namespace k2
{

//
//  Represents a class containing state of K2 Node Pool.
//
class NodePool
{
protected:
    std::unordered_map<CollectionId, CollectionPtr> collections;
    std::unordered_map<ModuleId, std::unique_ptr<IModule>> modules;
    std::atomic_bool collectionLock{false};

    class LockScope
    {
        std::atomic_bool &lock;

    public:
        LockScope(std::atomic_bool &lock) : lock(lock)
        {
            bool expected = false;
            while (!lock.compare_exchange_strong(expected, true)) {
                expected = false;
            }
        }

        ~LockScope() { lock = false; }
    };

    Status _internalizeCollection(CollectionMetadata &&metadata,
                                  Collection *&ptr)
    {
        //
        //  We store collection metadata per Pool, so Nodes will share it.
        //  Thus we need to have a lock to synchronize access between Nodes.
        //  It's not crucial since it happens only during partition assignment
        //
        LockScope lockScope(collectionLock);

        auto colIt = collections.find(metadata.getId());
        if (colIt != collections.end()) {
            ptr = colIt->second.get();
            return Status::Ok;
        }

        auto moduleIt = modules.find(metadata.getModuleId());
        if (moduleIt == modules.end())
            return Status::ModuleIsNotRegistered;

        CollectionPtr newCollection(std::move(metadata), *moduleIt->second);
        ModuleResponse moduleResponse = moduleIt->second->onNewCollection(
            *newCollection.get()); //  Minimal work should be done here to
                                   //  prevent deadlocks
        if (moduleResponse.type != ModuleResponse::Ok)
            return Status::ModuleRejectedCollection;

        ptr = newCollection.get();
        collections[newCollection.get()->getId()] = std::move(newCollection);

        return Status::Ok;
    }

public:
    Status internalizeCollection(CollectionMetadata &&metadata,
                                 Collection *&ptr)
    {
        RET(_internalizeCollection(std::move(metadata), ptr));
    }

    Status registerModule(ModuleId moduleId, std::unique_ptr<IModule> &&module)
    {
        auto emplaceResult = modules.try_emplace(moduleId, std::move(module));
        return emplaceResult.second
                   ? Status::Ok
                   : LOG_ERROR(Status::ModuleWithSuchIdAlreadyRegistered);
    }
};

} //  namespace k2
