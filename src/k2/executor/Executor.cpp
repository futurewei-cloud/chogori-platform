#include "Executor.h"

namespace k2
{

void Executor::init(const client::ClientSettings& settings)
{
    _settings = settings;

    k2::ServicePlatform::Settings platformSettings;
    platformSettings._useUserThread = false;
    if(!_launchers.empty()) {
        platformSettings._useUserThread = true;
    }

    _queues.reserve(settings.networkThreadCount);
    for(int i=0; i<settings.networkThreadCount; i++) {
        _queues.push_back(std::move(std::make_unique<ExecutorQueue>(platformSettings._useUserThread)));
    }

    auto pLauncher = std::unique_ptr<IServiceLauncher>(new MessageService::Launcher(_queues));
    _platform.init(std::move(platformSettings), _argv);
    _platform.registerService(std::ref(*pLauncher));
    _launchers.push_back(std::move(pLauncher));
}

void Executor::init(const client::ClientSettings& settings, std::shared_ptr<config::NodePoolConfig> pNodePoolConfig)
{
    if(pNodePoolConfig->getTransport()->isRdmaEnabled()) {
        _argv.push_back("--rdma");
        _argv.push_back(pNodePoolConfig->getTransport()->getRdmaNicId().c_str());
    }
    const std::string memorySize = pNodePoolConfig->getMemorySize();
    if(!memorySize.empty()) {
        _argv.push_back("-m");
	    _argv.push_back(memorySize.c_str());
    }
    if(pNodePoolConfig->isHugePagesEnabled()) {
        _argv.push_back("--hugepages");
    }

    // TODO: update to use n cores
	//_argv.push_back("--cpuset");
	//_argv.push_back("30");

    init(settings);
}

}; // namespace k2
