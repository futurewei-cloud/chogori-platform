#pragma once

// std
#include <sstream>
// k2
#include <k2/common/Common.h>
#include <k2/node/NodeConfig.h>
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
private:
    const std::string ID_TOKEN = "id";
    const std::string NODE_POOLS_TOKEN = "node_pools";
    const std::string TRANSPORT_TOKEN = "transport";

public:
    ConfigParser201907()
    {
        // empty
    }

    virtual std::shared_ptr<Config> parseConfig(const YAML::Node& yamlConfig)
    {
        std::shared_ptr<Config> pConfig = std::make_shared<Config>();
        pConfig->_schema = YamlUtils::getRequiredValue(yamlConfig, "schema", std::string(""));
        pConfig->_clusterName = YamlUtils::getOptionalValue(yamlConfig["cluster_name"], std::string(""));
        pConfig->_instanceVersion = YamlUtils::getOptionalValue(yamlConfig["instance_version"], 0);
        parseNodePools(YamlUtils::mergeAnchors(yamlConfig[NODE_POOLS_TOKEN]), pConfig->_nodePoolMap);
        parseCluster(yamlConfig, pConfig);
        parseClient(yamlConfig["client"], pConfig->_pClientNodePoolConfig);

        return pConfig;
    }

protected:
    void parseNodePools(const YAML::Node& yamlConfig, std::map<std::string, std::shared_ptr<NodePoolConfig>>& nodePoolMap)
    {
        if(!yamlConfig) {
            return;
        }

        int i=0;
        for(auto nodePool: yamlConfig) {
            const std::string id = YamlUtils::getOptionalValue(nodePool[ID_TOKEN], std::to_string(i));
            auto pNodePoolConfig = nodePoolMap.find(id) == nodePoolMap.end() ? nullptr : nodePoolMap.find(id)->second;
            if(!pNodePoolConfig) {
                pNodePoolConfig = std::make_shared<NodePoolConfig>();
                pNodePoolConfig->_id = id;
                nodePoolMap.insert(std::pair(id, pNodePoolConfig));
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

        const int nodesToCreate = pNodePoolConfig->getNodes().empty() ? YamlUtils::getOptionalValue(yamlConfig["node_count"], 1) : 0;
        pNodePoolConfig->_memorySize = YamlUtils::getOptionalValue(yamlConfig["memory_size"], pNodePoolConfig->_memorySize);
        pNodePoolConfig->_hugePagesEnabledFlag = YamlUtils::getOptionalValue(yamlConfig["enable_hugepages"], pNodePoolConfig->_hugePagesEnabledFlag);
        pNodePoolConfig->_monitoringEnabledFlag = YamlUtils::getOptionalValue(yamlConfig["enable_monitoring"], pNodePoolConfig->_monitoringEnabledFlag);
        parseTransport(YamlUtils::mergeAnchors(yamlConfig[TRANSPORT_TOKEN]), pNodePoolConfig->_pTransport);

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
            const size_t id = YamlUtils::getOptionalValue(node[ID_TOKEN], i);
            ASSERT(pNodePoolConfig->getNodes().size() > id);
            auto pNodeConfig = pNodePoolConfig->getNodes()[id];
            auto partitions = node["partitions"];
            if(partitions) {
                int j=0;
                for(auto partition :  partitions) {
                    std::string partitionId = YamlUtils::getOptionalValue(partition[ID_TOKEN], std::to_string(j));
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

            parseTransport(YamlUtils::mergeAnchors(node[TRANSPORT_TOKEN]), pNodeConfig->_pTransport);
            ++i;
        }
     }

    void parsePartition(const YAML::Node& yamlConfig, std::shared_ptr<PartitionConfig> pPartitionConfig)
    {
        if(!yamlConfig) {
            return;
        }

        const std::string id = YamlUtils::getOptionalValue(yamlConfig[ID_TOKEN], pPartitionConfig->_id);
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
        auto clusterNode = yamlConfig["cluster"];
        if(!clusterNode) {

            return;
        }

        for(auto hostNode: clusterNode) {
            std::string hostName = YamlUtils::getRequiredValue(hostNode, "host", std::string());
            auto nodePoolsNode = hostNode[NODE_POOLS_TOKEN];
            if(!nodePoolsNode) {
                continue;
            }

            std::map<std::string, std::shared_ptr<NodePoolConfig>> nodePoolMap;
            parseNodePools(yamlConfig[NODE_POOLS_TOKEN], nodePoolMap);

            for(auto nodePoolNode : nodePoolsNode) {
                auto it = nodePoolMap.find(YamlUtils::getRequiredValue(nodePoolNode, ID_TOKEN, std::string()));
                ASSERT(it!=nodePoolMap.end());
                auto pNodePoolConfig = it->second;
                parseNodePool(YamlUtils::mergeAnchors(nodePoolNode), pNodePoolConfig);
            }

            std::vector<std::shared_ptr<NodePoolConfig>> nodePools;
            std::transform(
                std::begin(nodePoolMap),
                std::end(nodePoolMap),
                std::back_inserter(nodePools), [](const auto& pair) { return pair.second; }
            );
            pConfig->_clusterMap.insert(std::pair(hostName, std::move(nodePools)));
        }
    }

    void parseClient(const YAML::Node& yamlConfig, std::shared_ptr<NodePoolConfig> pNodePoolConfig)
    {
        if(!yamlConfig) {
            return;
        }

        parseNodePool(YamlUtils::mergeAnchors(yamlConfig["node_pool"]), pNodePoolConfig);
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
            pTransport->_rdmaAddress = YamlUtils::getOptionalValue(node["address"], pTransport->_rdmaAddress);
            pTransport->_rdmaNicId = YamlUtils::getOptionalValue(node["nic_id"], pTransport->_rdmaNicId);
        }
    }

}; // class ConfigParser201907

} // namespace config
} // namespace k2

