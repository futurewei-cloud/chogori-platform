#pragma once

// k2
#include <common/Common.h>
#include <node/NodeConfig.h>
// k2:config
#include "YamlUtils.h"
#include "Config.h"
#include "Cluster.h"

namespace k2
{
namespace config
{

class ConfigParser201907: public IConfigParser
{
public:
    ConfigParser201907()
    {
        // empty
    }

    virtual std::shared_ptr<Config> parseConfig(const YAML::Node& yamlConfig)
    {
        std::shared_ptr<Config> pConfig = std::make_shared<Config>();
        pConfig->_schema = YamlUtils::getRequiredValue(yamlConfig, "schema", std::string(""));
        pConfig->_clusterName = YamlUtils::getOptionalValue(yamlConfig["cluster-name"], std::string(""));
        pConfig->_instanceVersion = YamlUtils::getOptionalValue(yamlConfig["instance-version"], 0);
        pConfig->_nodePoolMap = std::move(parseNodePools(yamlConfig["node-pools"]));
        parseCluster(yamlConfig["cluster"], pConfig);

        return pConfig;
    }

    std::map<std::string, std::shared_ptr<NodePoolConfig>> parseNodePools(const YAML::Node& yamlConfig)
    {
        std::vector<std::shared_ptr<NodePoolConfig>> nodePools;
        std::map<std::string, std::shared_ptr<NodePoolConfig>> nodeMap;

        int i=0;
        for(auto n: yamlConfig) {
            auto poolNode = YamlUtils::mergeAnchors(n);
            std::shared_ptr<NodePoolConfig> pNodePoolConfig = std::make_shared<NodePoolConfig>();
            pNodePoolConfig->_id = std::to_string(i);
            parseNodePool(poolNode, pNodePoolConfig);
            nodeMap.insert(std::pair(pNodePoolConfig->_id, pNodePoolConfig));
            i++;
        }

        return std::move(nodeMap);
    }

    void parseNodePool(const YAML::Node& yamlConfig, std::shared_ptr<NodePoolConfig> pNodePoolConfig)
    {
        if(!yamlConfig) {
            return;
        }

        const std::string id = YamlUtils::getOptionalValue(yamlConfig["id"], pNodePoolConfig->_id);
        const int nodesToCreate = pNodePoolConfig->getNodes().empty() ? YamlUtils::getOptionalValue(yamlConfig["node-count"], 1) : 0;
        pNodePoolConfig->_id = id;
        parseTransport(YamlUtils::mergeAnchors(yamlConfig["transport"]), pNodePoolConfig->_pTransport);

        // create all nodes based on the count
        for(uint16_t count=0; count < nodesToCreate; ++count) {
            auto pNodeConfig = std::make_shared<NodeConfig>();
            pNodeConfig->_nodeId = count;
            *(pNodeConfig->_pTransport) = *(pNodePoolConfig->_pTransport);
            pNodePoolConfig->_nodes.push_back(pNodeConfig);
        }

        parseNodes(YamlUtils::mergeAnchors(yamlConfig["nodes"]), pNodePoolConfig);
    }

     void parseNodes(const YAML::Node& yamlConfig, std::shared_ptr<NodePoolConfig> pNodePoolConfig)
     {
        if(!yamlConfig) {
            return;
        }

        int i=0;
        for(auto n: yamlConfig) {
            auto node = YamlUtils::mergeAnchors(n);
            const size_t id = YamlUtils::getOptionalValue(node["id"], i);
            ASSERT(pNodePoolConfig->getNodes().size() > id);
            auto pNodeConfig = pNodePoolConfig->getNodes()[id];
            auto partitions = node["partitions"];
            if(partitions) {
                for(size_t j=0; j<partitions.size(); j++) {
                    pNodeConfig->_partitions.push_back(partitions[j].as<std::string>());
                }
            }

            parseTransport(YamlUtils::mergeAnchors(node["transport"]), pNodeConfig->_pTransport);
            ++i;
        }
     }

    void parseCluster(const YAML::Node& yamlConfig, std::shared_ptr<Config> pConfig)
    {
        if(!yamlConfig) {
            return;
        }

        for(auto host: yamlConfig) {
            int i;
            for(auto nodePool: YamlUtils::mergeAnchors(host["node-pools"])) {
                const std::string id = YamlUtils::getOptionalValue(nodePool["id"], std::to_string(i));
                auto pNodePoolConfig = pConfig->getNodePool(id);
                if(!pNodePoolConfig) {
                    pNodePoolConfig = std::make_shared<NodePoolConfig>();
                    pConfig->_nodePoolMap.insert(std::pair(id, pNodePoolConfig));
                }
                // apply the nodepool overrides
                parseNodePool(nodePool, pNodePoolConfig);
                ++i;
            }
        }
    }

    void parseTransport(const YAML::Node& yamlConfig, std::shared_ptr<Transport> pTransport)
    {
        auto yaml = YamlUtils::mergeAnchors(yamlConfig);

        auto node = yaml["tcp"];
        if(node) {
            pTransport->_tcpPort = YamlUtils::getOptionalValue(node["port"], 11311);
            pTransport->_tcpAddress = YamlUtils::getOptionalValue(node["address"], std::string("0.0.0.0"));
        }

        node = yaml["rdma"];
        if(node) {
            pTransport->_rdmaPort = YamlUtils::getOptionalValue(node["port"], 0);
            pTransport->_rdmaAddress = YamlUtils::getOptionalValue(node["address"], std::string(""));
        }
    }



}; // class ConfigParser201907

} // namespace config
} // namespace k2

