#pragma once

#include "NodePool.h"
#include "Node.h"

namespace k2
{

//
//  Represents a class containing state of K2 Node Pool.
//
class NodePoolImpl : public INodePool
{
    DISABLE_COPY_MOVE(NodePoolImpl)

    PoolMonitor monitor;

public:
    NodePoolImpl() : monitor(*this)
    {
        monitorPtr = &monitor;
        name = "K2Pool_" + std::to_string(getpid()); //  TODO: add ip and some randomization
    }

    Status registerModule(ModuleId moduleId, std::unique_ptr<IModule>&& module)
    {
        auto emplaceResult = modules.try_emplace(moduleId, std::move(module));
        return emplaceResult.second ? Status::Ok : LOG_ERROR(Status::ModuleWithSuchIdAlreadyRegistered);
    }

    Status registerNode(std::unique_ptr<Node> node)
    {
        nodes.push_back(node.release());
        return Status::Ok;
    }

    void setCurrentNodeLocationInfo(std::vector<String> endpoints, int coreId)
    {
        String nodeName = name + "_" + std::to_string(getScheduingPlatform().getCurrentNodeId());
        getCurrentNode().setLocationInfo(std::move(nodeName), std::move(endpoints), coreId);
    }

    void setScheduingPlatform(ISchedulingPlatform* platform)
    {
        ASSERT(!schedulingPlatform);
        ASSERT(platform);
        schedulingPlatform = platform;
    }

    NodePoolConfig& getConfig() { return config; }

    PoolMonitor& getMonitor() { return monitor; }

    void completeInitialization()
    {
        getMonitor().start();
    }

    ~NodePoolImpl()
    {
        for(auto& node : nodes)
        {
            delete node;
            node = nullptr;
        }
        nodes.clear();
    }
};

}   //  namespace k2
