#pragma once

// yaml
#include <yaml-cpp/yaml.h>
// k2
#include "Config.h"

namespace k2
{

class ConfigLoader
{
public:
    static k2_shared_ptr<Config> loadConfig(const std::string& configFile)
    {
        YAML::Node config = YAML::LoadFile(configFile);
        k2_shared_ptr<Config> pConfig = k2_make_shared<Config>();
        k2_shared_ptr<NodePoolConfig> pNodePoolConfig = k2_make_shared<NodePoolConfig>();
        // only one nodepool supported
        pConfig->addNodePool(pNodePoolConfig);

        for(uint16_t count=0; count<config["nodes_count"].as<uint16_t>(0); ++count)
        {
            auto pNodeConfig = k2_make_shared<NodeEndpointConfig>();
            // TODO: map the endpoint type; fixing it to IPv4 for the moment
            pNodeConfig->type = NodeEndpointConfig::IPv4;
            pNodeConfig->ipv4.address = ntohl((uint32_t)inet_addr(config["address"].as<std::string>().c_str()));
            pNodeConfig->ipv4.port = config["nodes_minimum_port"].as<uint16_t>(0) + count;
            pNodePoolConfig->addNode(pNodeConfig);
        }

        auto partitionManagerNode = config["partitionManagerSet"];
        if(partitionManagerNode) {
            for(YAML::Node node : partitionManagerNode)
            {
                // address field is of form "<ipv4>:<port>":
                std::string partitionManager = node["address"].as<std::string>();
                pNodePoolConfig->partitionManagerSet.push_back(partitionManager);
            }
        }
        else {
            K2INFO("Partition manager not defined in the configration");
        }

        pNodePoolConfig->monitorEnabled = getOptionalValue(config["monitorEnabled"], pNodePoolConfig->monitorEnabled);
        pNodePoolConfig->rdmaEnabled = getOptionalValue(config["rdmaEnabled"], pNodePoolConfig->rdmaEnabled);
        if (config["nodes_cpu_set"])
        {
            pNodePoolConfig->cpuSetStr = getRequiredValue(config, "nodes_cpu_set", std::string("0"));
        }
        pNodePoolConfig->cpuSetGeneralStr = getOptionalValue(config["pool_cpu_set"], pNodePoolConfig->cpuSetGeneralStr);
        pNodePoolConfig->rdmaNicId = getOptionalValue(config["nic_id"], pNodePoolConfig->rdmaNicId);
        pNodePoolConfig->memorySizeStr = getOptionalValue(config["memory"], pNodePoolConfig->memorySizeStr);
        pNodePoolConfig->hugePagesEnabled = getOptionalValue(config["hugepages"], pNodePoolConfig->hugePagesEnabled);

        return pConfig;
    }

    template<class T>
    static T getOptionalValue(YAML::Node node, T defaultValue)
    {
        return node ? node.as<T>(defaultValue) : defaultValue;
    }

    template<class T>
    static T getRequiredValue(YAML::Node node, const std::string& name, T dataType)
    {
        YAML::Node valueNode = node[name];
        if(!valueNode) {
            K2ERROR("Config; missing config value:" << name);
            ASSERT(valueNode);
        }

        return getOptionalValue(valueNode, dataType);
    }

}; // class ConfigLoader

}; // namespace k2
