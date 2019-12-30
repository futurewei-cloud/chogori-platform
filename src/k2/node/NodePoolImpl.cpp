#include "NodePoolImpl.h"
namespace k2
{
NodePoolImpl::NodePoolImpl() : monitor(*this)
{
    monitorPtr = &monitor;
    name = "K2Pool_" + std::to_string(getpid()); //  TODO: add ip and some randomization
}

Status NodePoolImpl::registerModule(ModuleId moduleId, std::unique_ptr<IModule> module)
{
    auto emplaceResult = modules.try_emplace(moduleId, std::move(module));
    return emplaceResult.second ? Status::Ok : Status::ModuleWithSuchIdAlreadyRegistered;
}

Status NodePoolImpl::registerNode(std::unique_ptr<Node> node)
{
    nodes.push_back(node.release());
    return Status::Ok;
}

void NodePoolImpl::setCurrentNodeLocationInfo(std::vector<String> endpoints, int coreId)
{
    String nodeName = name + "_" + std::to_string(getSchedulingPlatform().getCurrentNodeId());
    getCurrentNode().setLocationInfo(std::move(nodeName), std::move(endpoints), coreId);
}

void NodePoolImpl::setScheduingPlatform(ISchedulingPlatform* platform)
{
    assert(!schedulingPlatform);
    assert(platform);
    schedulingPlatform = platform;
}

NodePoolConfig& NodePoolImpl::getConfig() { return config; }

PoolMonitor& NodePoolImpl::getMonitor() { return monitor; }

void NodePoolImpl::completeInitialization()
{
    getMonitor().start();
}

NodePoolImpl::~NodePoolImpl()
{
    for(auto& node : nodes)
    {
        delete node;
        node = nullptr;
    }
    nodes.clear();
}

}   //  namespace k2
