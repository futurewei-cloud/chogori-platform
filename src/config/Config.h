#pragma once

// k2
#include <common/Common.h>
// k2:config
#include "NodePoolConfig.h"
#include "PartitionManagerConfig.h"

namespace k2
{
namespace config
{

class Config
{
friend class ConfigParserLegacy;
protected:
    uint64_t _instanceVersion;
    std::vector<std::shared_ptr<NodePoolConfig>> _nodePools;
    std::shared_ptr<PartitionManagerConfig> _pPartitionManager;

public:
    const std::vector<std::shared_ptr<NodePoolConfig>>& getNodePools() const
    {
        return _nodePools;
    }

    const std::shared_ptr<PartitionManagerConfig> getPartitionManager() const
    {
        return _pPartitionManager;
    }

}; // class Config

}; // namespace config
}; // namespace k2
