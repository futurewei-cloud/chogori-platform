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

protected:
    Transport _transport;
    int _nodeId;

public:
    const Transport& getTransport() const
    {
        return _transport;
    }

}; // class NodeConfig

}; // namespace config
}; // namespace k2
