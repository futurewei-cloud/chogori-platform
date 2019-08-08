#pragma once

// std
#include <vector>
// k2:config
#include "NodeConfig.h"

namespace k2
{
namespace config
{

class NodePoolConfig
{
friend class ConfigParserLegacy;
friend class Config;
protected:
    std::vector<std::shared_ptr<NodeConfig>> _nodes;
    Transport _transport;
    std::string _cpuSet;
    bool _monitoringEnabledFlag = false;
    // memory
    bool _hugePagesEnabledFlag = false;
    std::string _memorySize;

public:
    const std::vector<std::shared_ptr<NodeConfig>>& getNodes() const
    {
        return _nodes;
    }

    bool isMonitoringEnabled() const
    {
        return _monitoringEnabledFlag;
    }

    bool isHugePagesEnabled() const
    {
        return _hugePagesEnabledFlag;
    }

    const Transport& getTransport()
    {
        return _transport;
    }

    const std::string& getCpuSet() const
    {
        return _cpuSet;
    }

    const std::string& getMemorySize() const
    {
        return _memorySize;
    }

}; // class NodePoolConfig

}; // namespace config
}; // namespace k2
