#pragma once

// k2:config
#include "Transport.h"

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
    std::vector<std::string> _partitions;
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

    const std::vector<std::string>& getPartitions() const
    {
        return _partitions;
    }

}; // class NodeConfig

}; // namespace config
}; // namespace k2
