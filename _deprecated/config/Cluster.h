#pragma once

// k2
#include <k2/common/Common.h>
// k2:config
#include "NodePoolConfig.h"
#include "PartitionManagerConfig.h"

#include <map>

namespace k2
{
namespace config
{

class Cluster
{
friend class ConfigParser201907;

protected:
    std::map<std::string, std::vector<std::shared_ptr<NodePoolConfig>>> _hostNodepools;

public:
    Cluster()
    {
        // empty
    }

}; // class Cluster

}; // namespace config
}; // namespace k2
