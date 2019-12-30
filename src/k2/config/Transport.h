#pragma once

// std
#include <string>
// boost
#include <boost/format.hpp>

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
    std::string _tcpAddress = "0.0.0.0";
    uint64_t _tcpPort = 11311;
    // rdma
    std::string _rdmaAddress;
    uint64_t _rdmaPort = 0;
    std::string _rdmaNicId;

public:
    Transport()
    {
        // empty
    }

    Transport(const std::string& tcpAddress, uint64_t tcpPort, const std::string& rdmaAddress, uint64_t rdmaPort, const std::string& rdmaNicId)
    : _tcpAddress(tcpAddress)
    , _tcpPort(tcpPort)
    , _rdmaAddress(rdmaAddress)
    , _rdmaPort(rdmaPort)
    , _rdmaNicId(rdmaNicId)
    {
        // empty
    }

    const std::string& getTcpAddress() const
    {
        return _tcpAddress;
    }

    uint64_t getTcpPort() const
    {
        return _tcpPort;
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

    bool isTcpEnabled() const
    {
        return (_tcpPort > 0 && !_tcpAddress.empty());
    }

    bool isRdmaEnabled() const
    {
        return (!_rdmaNicId.empty() || (_rdmaPort >0 && _rdmaAddress.empty()));
    }

    std::string getEndpoint() const
    {
        std::string formatStr = "+k2rpc://%s:%d";
        if(isRdmaEnabled()) {
            formatStr = "rrdma" + formatStr;

            return boost::str(boost::format(formatStr) % _rdmaAddress % _rdmaPort);
        }

        formatStr = "tcp" + formatStr;

        return boost::str(boost::format(formatStr) % _tcpAddress % _tcpPort);
    }

}; // class Transport

}; // namespace config
}; // namespace k2
