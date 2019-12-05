#pragma once

// k2:config
#include "Transport.h"
#include "PartitionConfig.h"

#include <map>
#include <string>

namespace k2
{
namespace config
{

class NodeConfig
{
friend class NodePoolConfig;
friend class ConfigParserLegacy;
friend class ConfigParser201907;

protected:
    std::shared_ptr<Transport> _pTransport;
    std::map<std::string, std::shared_ptr<PartitionConfig>> _partitionsMap;
    int _nodeId;

public:
    NodeConfig()
    : _pTransport(std::make_shared<Transport>())
    {
        // empty
    }

    const std::shared_ptr<Transport> getTransport() const
    {
        return _pTransport;
    }

    const std::vector<std::shared_ptr<PartitionConfig>> getPartitions() const
    {
        std::vector<std::shared_ptr<PartitionConfig>> partitions;

        std::transform(
            std::begin(_partitionsMap),
            std::end(_partitionsMap),
            std::back_inserter(partitions),
            [](const auto& pair) {
                return pair.second;
            }
        );

        return std::move(partitions);
    }

    const std::shared_ptr<PartitionConfig> getPartition(const std::string& id)
    {
         auto pair = _partitionsMap.find(id);

        return (pair!=_partitionsMap.end()) ? pair->second : nullptr;
    }

}; // class NodeConfig

}; // namespace config
}; // namespace k2
