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
    NodePoolImpl();

    Status registerModule(ModuleId moduleId, std::unique_ptr<IModule> module);

    Status registerNode(std::unique_ptr<Node> node);

    void setCurrentNodeLocationInfo(std::vector<String> endpoints, int coreId);

    void setScheduingPlatform(ISchedulingPlatform* platform);

    NodePoolConfig& getConfig();

    PoolMonitor& getMonitor();

    void completeInitialization();

    ~NodePoolImpl();
};

}   //  namespace k2
