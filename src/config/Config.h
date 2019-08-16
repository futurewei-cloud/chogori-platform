#pragma once

// k2
#include <common/Common.h>
// k2:config
#include "NodePoolConfig.h"
#include "PartitionManagerConfig.h"
#include "ClusterConfig.h"

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
    std::shared_ptr<PartitionManagerConfig> _pPartitionManager;

public:
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

    const std::shared_ptr<PartitionManagerConfig> getPartitionManager() const
    {
        return _pPartitionManager;
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

}; // namespace config
}; // namespace k2
