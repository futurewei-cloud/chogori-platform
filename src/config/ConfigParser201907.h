#pragma once

// std
#include <sstream>
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
        parseNodePools(YamlUtils::mergeAnchors(yamlConfig["node-pools"]), pConfig);
        parseCluster(yamlConfig["cluster"], pConfig);

        return pConfig;
    }

    void parseNodePools(const YAML::Node& yamlConfig, std::shared_ptr<Config> pConfig)
    {
        if(!yamlConfig) {
            return;
        }

        std::vector<std::shared_ptr<NodePoolConfig>> nodePools;
        std::map<std::string, std::shared_ptr<NodePoolConfig>> nodeMap;

        int i=0;
        for(auto nodePool: yamlConfig) {
            const std::string id = YamlUtils::getOptionalValue(nodePool["id"], std::to_string(i));
            auto pNodePoolConfig = pConfig->getNodePool(id);
            if(!pNodePoolConfig) {
                pNodePoolConfig = std::make_shared<NodePoolConfig>();
                pNodePoolConfig->_id = id;
                pConfig->_nodePoolMap.insert(std::pair(id, pNodePoolConfig));
            }
            // apply the nodepool overrides
            parseNodePool(YamlUtils::mergeAnchors(nodePool), pNodePoolConfig);

            ++i;
        }
    }

    void parseNodePool(const YAML::Node& yamlConfig, std::shared_ptr<NodePoolConfig> pNodePoolConfig)
    {
        if(!yamlConfig) {
            return;
        }

        const int nodesToCreate = pNodePoolConfig->getNodes().empty() ? YamlUtils::getOptionalValue(yamlConfig["node-count"], 1) : 0;
        parseTransport(YamlUtils::mergeAnchors(yamlConfig["transport"]), pNodePoolConfig->_pTransport);

        // create all nodes based on the count
        for(uint16_t count=0; count < nodesToCreate; ++count) {
            auto pNodeConfig = pNodePoolConfig->getNode(count);
            if(!pNodeConfig) {
                pNodeConfig = std::make_shared<NodeConfig>();
                pNodeConfig->_nodeId = count;
                *(pNodeConfig->_pTransport) = *(pNodePoolConfig->_pTransport);
                pNodePoolConfig->_nodes.push_back(pNodeConfig);
            }
        }

        parseNodes(yamlConfig["nodes"], pNodePoolConfig);
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
                int j=0;
                for(auto partition :  partitions) {
                    std::string partitionId = YamlUtils::getOptionalValue(partition["id"], std::to_string(j));
                    auto pPartitionConfig = pNodeConfig->getPartition(partitionId);
                    if(!pPartitionConfig) {
                        pPartitionConfig = std::make_shared<PartitionConfig>();
                        pPartitionConfig->_id = partitionId;
                        pNodeConfig->_partitionsMap.insert(std::pair(partitionId, pPartitionConfig));
                    }
                    parsePartition(partition, pPartitionConfig);
                    ++j;
                }
            }

            parseTransport(YamlUtils::mergeAnchors(node["transport"]), pNodeConfig->_pTransport);
            ++i;
        }
     }

    void parsePartition(const YAML::Node& yamlConfig, std::shared_ptr<PartitionConfig> pPartitionConfig)
    {
        if(!yamlConfig) {
            return;
        }

        const std::string id = YamlUtils::getOptionalValue(yamlConfig["id"], pPartitionConfig->_id);
        const std::string range = YamlUtils::getOptionalValue(yamlConfig["range"], std::string());
        if(!range.empty()) {
            std::istringstream stream(range);
            std::string token;
            while (getline(stream, token, '-')) {
                const char firstChar = token.c_str()[0];
                const char lastChar = token.c_str()[token.size()-1];
                if(firstChar == '[') {
                    pPartitionConfig->_range._lowerBoundClosed = true;
                }
                else if(firstChar == ']') {
                    pPartitionConfig->_range._upperBoundClosed = true;
                }
                std::string bound = token;
                if(firstChar == '[' || firstChar == '(') {
                    pPartitionConfig->_range._lowerBound = token.substr(1);
                }
                if(lastChar == ']' || lastChar == ')') {
                    pPartitionConfig->_range._upperBound = token.substr(0, token.size()-1);
                }
            }
        }

        pPartitionConfig->_id = id;
    }

    void parseCluster(const YAML::Node& yamlConfig, std::shared_ptr<Config> pConfig)
    {
        if(!yamlConfig) {
            return;
        }

        for(auto host: yamlConfig) {
            parseNodePools(host["node-pools"], pConfig);
        }
    }

    void parseTransport(const YAML::Node& yamlConfig, std::shared_ptr<Transport> pTransport)
    {
        if(!yamlConfig) {
            return;
        }

        auto yaml = YamlUtils::mergeAnchors(yamlConfig);

        auto node = yaml["tcp"];
        if(node) {
            pTransport->_tcpPort = YamlUtils::getOptionalValue(node["port"], pTransport->_tcpPort);
            pTransport->_tcpAddress = YamlUtils::getOptionalValue(node["address"], pTransport->_tcpAddress);
        }

        node = yaml["rdma"];
        if(node) {
            pTransport->_rdmaPort = YamlUtils::getOptionalValue(node["port"], pTransport->_rdmaPort);
            pTransport->_rdmaAddress = YamlUtils::getOptionalValue(node["address"],  pTransport->_rdmaAddress);
        }
    }

}; // class ConfigParser201907

} // namespace config
} // namespace k2

