#pragma once

// k2
#include <k2/common/Common.h>
// k2:config
#include "NodePoolConfig.h"
#include "PartitionManagerConfig.h"

namespace k2
{
namespace config
{

class ClusterConfig
{
friend class ConfigParserLegacy;
friend class ConfigParser201907;

protected:
    std::vector<std::shared_ptr<NodePoolConfig>> _nodePools;

public:
    const std::vector<std::shared_ptr<NodePoolConfig>>& getNodePools() const
    {
        return _nodePools;
    }

}; // class ClusterConfig

}; // namespace config
}; // namespace k2
