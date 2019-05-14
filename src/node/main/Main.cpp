#include "node/NodePool.h"
#include "node/module/MemKVModule.h"
#include "K2TXPlatform.h"
#include <yaml-cpp/yaml.h>

using namespace k2;

// TODO: Move the configuration logic into a separate file.
void loadConfig(NodePool& pool, const std::string& configFile)
{
    YAML::Node config = YAML::LoadFile(configFile);

    for(YAML::Node node : config["nodes"])
    {
        NodeEndpointConfig nodeConfig;
        // TODO: map the endpoint type; fixing it to IPv4 for the moment
        nodeConfig.type = NodeEndpointConfig::IPv4;
        nodeConfig.ipv4.address = ntohl((uint32_t)inet_addr(node["endpoint"]["ip"].as<std::string>().c_str()));
        nodeConfig.ipv4.port = node["endpoint"]["port"].as<uint16_t>();

        TIF(pool.registerNode(nodeConfig));
    }
}

int main(int argc, char** argv)
{
    (void)argc; // TODO use me
    (void)argv; // TODO use me
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

        k2::NodePool pool;
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
            TIF(pool.registerNode(nodeConfig));
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
