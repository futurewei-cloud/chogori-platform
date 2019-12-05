#pragma once

// k2
#include <k2/common/Common.h>

// k2:config
#include "NodePoolConfig.h"
#include "PartitionManagerConfig.h"
#include "ClusterConfig.h"

// third-party
#include <boost/program_options.hpp>
#include <seastar/core/distributed.hh>  // for distributed<>

namespace k2
{
namespace config
{

class Config
{
friend class ConfigParserLegacy;
friend class ConfigParser201907;

protected:
    uint64_t _instanceVersion;
    std::string _schema;
    std::string _clusterName;
    std::map<std::string, std::shared_ptr<NodePoolConfig>> _nodePoolMap;
    std::map<std::string, std::vector<std::shared_ptr<NodePoolConfig>>> _clusterMap;
    std::shared_ptr<PartitionManagerConfig> _pPartitionManager;
    std::shared_ptr<NodePoolConfig> _pClientNodePoolConfig;

public:
    Config()
    : _pClientNodePoolConfig(std::make_shared<NodePoolConfig>())
    {
        // empty
    }

    const std::vector<std::shared_ptr<NodePoolConfig>> getNodePools() const
    {
        std::vector<std::shared_ptr<NodePoolConfig>> nodePools;
        std::transform(
            std::begin(_nodePoolMap),
            std::end(_nodePoolMap),
            std::back_inserter(nodePools),
            [](const auto& pair) {
                return pair.second;
            }
        );

        return std::move(nodePools);
    }

    const std::shared_ptr<NodePoolConfig> getNodePool(const std::string& id) const
    {
        auto pair = _nodePoolMap.find(id);

        return (pair!=_nodePoolMap.end()) ? pair->second : nullptr;
    }

    const std::vector<std::shared_ptr<PartitionConfig>> getPartitions() const
    {
        std::vector<std::shared_ptr<PartitionConfig>> partitions;

        for(auto pNodePoolConfig : getNodePools()) {
            for(auto pNodeConfig : pNodePoolConfig->getNodes()) {
                auto vector = std::move(pNodeConfig->getPartitions());
                partitions.insert(partitions.end(), vector.begin(), vector.end());
            }
        }

        return std::move(partitions);
    }

    const std::vector<std::shared_ptr<NodeConfig>> getClusterNodes() const
    {
        std::vector<std::shared_ptr<NodeConfig>> vector;

        for(auto pair : _clusterMap) {
            for(auto pNodePoolConfig : pair.second) {
                for(auto pNodeConfig : pNodePoolConfig->getNodes()) {
                    vector.push_back(pNodeConfig);
                }
            }
        }

        return std::move(vector);
    }

    const std::vector<std::shared_ptr<NodePoolConfig>> getNodePoolsForHost(const std::string& hostname)
    {
        auto it = _clusterMap.find(hostname);
        if(it==_clusterMap.end()) {
            return std::move(std::vector<std::shared_ptr<NodePoolConfig>>());
        }

        return it->second;
    }

    const std::shared_ptr<PartitionManagerConfig> getPartitionManager() const
    {
        return _pPartitionManager;
    }

    const std::shared_ptr<NodePoolConfig> getClientConfig() const
    {
        return _pClientNodePoolConfig;
    }

    const std::string& getSchema() const
    {
        return _schema;
    }

    const std::string& getClusterName() const
    {
        return _clusterName;
    }

    uint64_t getInstanceVersion() const
    {
        return _instanceVersion;
    }

}; // class Config
typedef boost::program_options::variables_map BPOVarMap;
typedef seastar::distributed<BPOVarMap> BPOConfigMapDist_t;
} // namespace config

// for convenient access to globally initialized configuration
extern config::BPOConfigMapDist_t ___config___;
inline config::BPOConfigMapDist_t& ConfigDist() { return ___config___; }
inline const config::BPOVarMap& Config() { return ___config___.local(); }
} // namespace k2
