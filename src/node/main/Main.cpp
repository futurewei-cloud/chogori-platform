#include "node/NodePoolImpl.h"
#include "node/module/MemKVModule.h"
#include "node/Node.h"
#include "K2TXPlatform.h"
#include <yaml-cpp/yaml.h>
#include <config/ConfigLoader.h>

using namespace k2;

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

        k2_shared_ptr<Config> pConfig = k2_make_shared<Config>();
        if(variablesMap.count("k2config"))
        {
            // load the configuration
            pConfig = ConfigLoader::loadConfig(variablesMap["k2config"].as<std::string>());
        }
        else
        {
            // create default configuration
            K2INFO("Using default configuration");
            auto pPoolConfig = k2_make_shared<NodePoolConfig>();
            pConfig->addNodePool(pPoolConfig);
            auto pNodeConfig = k2_make_shared<NodeEndpointConfig>();
            pPoolConfig->addNode(pNodeConfig);
            pNodeConfig->type = NodeEndpointConfig::IPv4;
            (pNodeConfig->ipv4).address = ntohl((uint32_t)inet_addr("0.0.0.0"));
            (pNodeConfig->ipv4).port = 11311;
        }

        auto nodePoolConfigs = std::move(pConfig->getNodePools());
        ASSERT(nodePoolConfigs.size() == 1); // currently we are only supporting one node pool
        auto pNodePoolConfig = nodePoolConfigs[0];
        auto nodeConfigs = std::move(pNodePoolConfig->getNodes());
        ASSERT(nodeConfigs.size() > 0); // we expect at least one endpoint

        std::vector<std::unique_ptr<k2::NodePoolImpl>> pools;
        for(auto pPoolConfig : nodePoolConfigs) {
            auto pPool = std::make_unique<k2::NodePoolImpl>(pPoolConfig);
            // this is KV module pool
            TIF(pPool->registerModule(ModuleId::Default, std::make_unique<k2::MemKVModule<MapIndexer>>()));
            for(auto pNodeConfig : nodeConfigs) {
                auto pNode = std::make_unique<Node>(*pPool, pNodeConfig);
                TIF(pPool->registerNode(std::move(pNode)));
            }
            pools.push_back(std::move(pPool));
        }

        k2::NodePoolImpl& pool = *(pools[0]);
        K2TXPlatform platform;
        pool.setScheduingPlatform(&platform);
        // blocking call
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
