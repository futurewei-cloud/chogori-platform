#pragma once

// k2
#include <common/Common.h>
#include <node/NodeConfig.h>
// k2:config
#include "YamlUtils.h"
#include "Config.h"

namespace k2
{
namespace config
{

class ConfigParserLegacy: public IConfigParser
{
public:
    ConfigParserLegacy()
    {
        // empty
    }

    virtual std::shared_ptr<Config> parseConfig(const YAML::Node& config)
    {
        std::shared_ptr<Config> pConfig = std::make_shared<Config>();
        // parse node pools
        parseNodePools(config, pConfig);
        // parse partition manager
        parsePartitionManager(config, pConfig);

        return pConfig;
    }

    void parseNodePools(const YAML::Node& config, std::shared_ptr<Config> pConfig)
    {
        std::shared_ptr<NodePoolConfig> pNodePoolConfig = std::make_shared<NodePoolConfig>();
        pConfig->_nodePools.push_back(pNodePoolConfig);

        pNodePoolConfig->_monitoringEnabledFlag = YamlUtils::getOptionalValue(config["monitorEnabled"], pNodePoolConfig->_monitoringEnabledFlag);

        //pNodePoolConfig->cpuSetGeneralStr = YamlUtils::getOptionalValue(config["pool_cpu_set"], pNodePoolConfig->cpuSetGeneralStr);
        if (config["nodes_cpu_set"])
        {
            pNodePoolConfig->_cpuSet = YamlUtils::getRequiredValue(config, "nodes_cpu_set", std::string("0"));
        }
        // rdma
        pNodePoolConfig->_transport._enableRdmaFlag = YamlUtils::getOptionalValue(config["rdmaEnabled"], pNodePoolConfig->_transport._enableRdmaFlag);
        pNodePoolConfig->_transport._rdmaNicId = YamlUtils::getOptionalValue(config["nic_id"], pNodePoolConfig->_transport._rdmaNicId);
        // memory
        pNodePoolConfig->_memorySize = YamlUtils::getOptionalValue(config["memory"], pNodePoolConfig->_memorySize);
        pNodePoolConfig->_hugePagesEnabledFlag = YamlUtils::getOptionalValue(config["hugepages"], pNodePoolConfig->_hugePagesEnabledFlag);
        // tcp
        pNodePoolConfig->_transport._tcpPort = YamlUtils::getOptionalValue(config["nodes_minimum_port"],  pNodePoolConfig->_transport._tcpPort);
        // parse the nodes for the nodepool
        parseNodes(config, pNodePoolConfig);
    }

    void parseNodes(const YAML::Node& config, std::shared_ptr<NodePoolConfig> pNodePoolConfig)
    {
        const int nodes = YamlUtils::getOptionalValue(config["nodes_count"], 1);
        for(uint16_t count=0; count<nodes; ++count) {
            auto pNodeConfig = std::make_shared<NodeConfig>();
            pNodePoolConfig->_nodes.push_back(pNodeConfig);
            pNodeConfig->_nodeId = count;
            pNodeConfig->_transport = pNodePoolConfig->_transport;
            pNodeConfig->_transport._tcpAddress = YamlUtils::getOptionalValue(config["address"], pNodePoolConfig->_transport._tcpAddress);
            pNodeConfig->_transport._tcpPort = pNodePoolConfig->_transport._tcpPort + count;
        }
    }

    void parsePartitionManager(const YAML::Node& config, std::shared_ptr<Config> pConfig)
    {
        std::shared_ptr<PartitionManagerConfig> pPartitionManager;
        auto partitionManagerNode = config["partitionManagerSet"];
        if(partitionManagerNode) {
            pPartitionManager = std::make_shared<PartitionManagerConfig>();
            for(YAML::Node node : partitionManagerNode) {
                // address field is of form "<ipv4>:<port>":
                std::string partitionManager = node["address"].as<std::string>();
                pPartitionManager->_endpoints.push_back(partitionManager);
                //pNodePoolConfig->partitionManagerSet.push_back(partitionManager);
            }
        }
        else {
            K2INFO("Partition manager not defined in the configration");
        }

        pConfig->_pPartitionManager = pPartitionManager;
    }

}; // class ConfigParser

} // namespace config
} // namespace k2
