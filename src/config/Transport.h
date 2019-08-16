#pragma once

// std
#include <string>

namespace k2
{
namespace config
{

class Transport
{
friend class ConfigParserLegacy;
friend class ConfigParser201907;
friend class NodePoolConfig;

protected:
    // tcp
    bool _enableTcpFlag = true;
    std::string _tcpAddress = "0.0.0.0";
    uint64_t _tcpPort = 11311;
    // rdma
    bool _enableRdmaFlag = false;
    std::string _rdmaAddress;
    uint64_t _rdmaPort;
    std::string _rdmaNicId;

public:

    bool isTcpEnabled() const
    {
        return _enableTcpFlag;
    }

    const std::string& getTcpAddress() const
    {
        return _tcpAddress;
    }

    uint64_t getTcpPort() const
    {
        return _tcpPort;
    }

    bool isRdmaEnabled() const
    {
        return _enableRdmaFlag;
    }

    const std::string& getRdmaNicId() const
    {
        return _rdmaNicId;
    }

    const std::string& getRdmaAddress() const
    {
        return _rdmaAddress;
    }

    uint64_t getRdmaPort() const
    {
        return _rdmaPort;
    }

}; // class Transport

}; // namespace config
}; // namespace k2
