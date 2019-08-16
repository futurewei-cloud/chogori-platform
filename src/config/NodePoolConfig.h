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
friend class ConfigParser201907;
friend class Config;
protected:
    std::string _id;
    std::shared_ptr<Transport> _pTransport;
    std::string _cpuSet;
    bool _monitoringEnabledFlag = false;
    // memory
    bool _hugePagesEnabledFlag = false;
    std::string _memorySize;
    std::vector<std::shared_ptr<NodeConfig>> _nodes;
    std::vector<std::string> _partitions;

public:
    NodePoolConfig()
    : _pTransport(std::make_shared<Transport>())
    {
        // empty
    }

    const std::string& getId() const
    {
        return _id;
    }

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

    const std::shared_ptr<Transport> getTransport() const
    {
        return _pTransport;
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
