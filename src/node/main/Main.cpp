#include "node/NodePoolImpl.h"
#include "node/module/MemKVModule.h"
#include "node/Node.h"
#include "K2TXPlatform.h"
#include <config/ConfigLoader.h>

using namespace k2;

// TODO: Move the configuration logic into a separate file.
void registerNodePool(NodePoolImpl& pool, std::shared_ptr<config::Config> pConfig)
{
    auto pNodePool = pConfig->getNodePools()[0];
    for(auto pNode : pNodePool->getNodes()) {
        NodeEndpointConfig nodeConfig;
        const auto pTransport = pNode->getTransport();
        if(pTransport->isTcpEnabled()) {
            // TODO: map the endpoint type; fixing it to IPv4 for the moment
            nodeConfig.type = NodeEndpointConfig::IPv4;
            nodeConfig.ipv4.address = ntohl((uint32_t)inet_addr(pTransport->getTcpAddress().c_str()));
            nodeConfig.ipv4.port = pTransport->getTcpPort();
        }

        TIF(pool.registerNode(std::make_unique<Node>(pool, std::move(nodeConfig))));
    }

    const auto pConfigManager = pConfig->getPartitionManager();
    if(pConfigManager)
    {
        // address field is of form "<ipv4>:<port>":
        pool.getConfig().partitionManagerSet.insert(pool.getConfig().partitionManagerSet.end(),
            pConfigManager->getEndpoints().begin(), pConfigManager->getEndpoints().end());
    }

    pool.getConfig().monitorEnabled = pNodePool->isMonitoringEnabled();
    pool.getConfig().rdmaEnabled = pNodePool->getTransport()->isRdmaEnabled();
    if (!pNodePool->getCpuSet().empty())
    {
        pool.getConfig().cpuSetStr = pNodePool->getCpuSet();
    }
    //pool.getConfig().cpuSetGeneralStr = config["pool_cpu_set"].as<std::string>();
    pool.getConfig().rdmaNicId = pNodePool->getTransport()->getRdmaNicId();
    pool.getConfig().memorySizeStr = pNodePool->getMemorySize();
    pool.getConfig().hugePagesEnabled = pNodePool->isHugePagesEnabled();
}

int main(int argc, char** argv)
{
    try
    {
        namespace bpo = boost::program_options;
        bpo::options_description k2Options("K2 Options");

        // get the k2 config from the command line
        k2Options.add_options()
            ("k2config", bpo::value<std::string>(), "k2 configuration file")
            ;

        // parse the command line options
        bpo::variables_map variablesMap;
        bpo::store(bpo::parse_command_line(argc, argv, k2Options), variablesMap);

        k2::NodePoolImpl pool;
        TIF(pool.registerModule(ModuleId::Default, std::make_unique<k2::MemKVModule<MapIndexer>>()));
        std::shared_ptr<config::Config> pConfig = variablesMap.count("k2config")
            ? config::ConfigLoader::loadConfig(variablesMap["k2config"].as<std::string>())
            : pConfig = config::ConfigLoader::loadDefaultConfig();

        // Register pool based on the configuration file
        registerNodePool(pool, pConfig);

        K2TXPlatform platform;
        pool.setScheduingPlatform(&platform);
        TIF(platform.run(pool));
    }
    catch(const Status& status)
    {
        LOG_ERROR(status);
        return (int)status;
    }
    catch(const std::runtime_error& re)
    {
        std::cerr << "Runtime error: " << re.what() << std::endl;
        return 1;
    }
    catch(const std::exception& ex)
    {
        std::cerr << "Error occurred: " << ex.what() << std::endl;
        return 1;
    }
    catch(...)
    {
        std::cerr << "Unknown exception is thrown." << std::endl;
        return 1;
    }

    return 0;
}
