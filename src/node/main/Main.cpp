#include "node/NodePoolImpl.h"
#include "node/module/MemKVModule.h"
#include "node/Node.h"
#include "K2TXPlatform.h"
#include <yaml-cpp/yaml.h>

using namespace k2;

// TODO: Move the configuration logic into a separate file.
void loadConfig(NodePoolImpl& pool, const std::string& configFile)
{
    YAML::Node config = YAML::LoadFile(configFile);

    for(uint16_t count=0; count<config["nodes_count"].as<uint16_t>(0); ++count)
    {
        NodeEndpointConfig nodeConfig;
        // TODO: map the endpoint type; fixing it to IPv4 for the moment
        nodeConfig.type = NodeEndpointConfig::IPv4;
        nodeConfig.ipv4.address = ntohl((uint32_t)inet_addr(config["address"].as<std::string>().c_str()));
        nodeConfig.ipv4.port = config["nodes_minimum_port"].as<uint16_t>(0) + count;

        TIF(pool.registerNode(std::make_unique<Node>(pool, std::move(nodeConfig))));
    }

    for(YAML::Node node : config["partitionManagerSet"])
    {
        std::string partitionManager = node["address"].as<std::string>();
        // port?
        pool.getConfig().partitionManagerSet.push_back(partitionManager);
    }

    pool.getConfig().monitorEnabled = config["monitorEnabled"].as<bool>(pool.getConfig().monitorEnabled);
    pool.getConfig().rdmaEnabled = config["rdmaEnabled"].as<bool>(pool.getConfig().rdmaEnabled);
    if (config["nodes_cpu_set"])
    {
        pool.getConfig().cpuSetStr = config["nodes_cpu_set"].as<std::string>();
    }
    pool.getConfig().cpuSetGeneralStr = config["pool_cpu_set"].as<std::string>();
    pool.getConfig().rdmaNicId = config["nic_id"].as<std::string>();
    pool.getConfig().memorySizeStr = config["memory"].as<std::string>();
    pool.getConfig().hugePagesEnabled = config["hugepages"].as<bool>(pool.getConfig().hugePagesEnabled);
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
        if(variablesMap.count("k2config"))
        {
            // Configure pool based on the configuration file
            loadConfig(pool, variablesMap["k2config"].as<std::string>());
        }
        else
        {
            // create default configuration
            NodeEndpointConfig nodeConfig;
            nodeConfig.type = NodeEndpointConfig::IPv4;
            nodeConfig.ipv4.address = ntohl((uint32_t)inet_addr("0.0.0.0"));
            nodeConfig.ipv4.port = 11311;
            TIF(pool.registerNode(std::make_unique<Node>(pool, std::move(nodeConfig))));
        }

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
