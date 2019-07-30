#pragma once

namespace k2
{

//
//  Address configuration for endpoint
//
class NodeEndpointConfig
{
friend class ConfigLoader;
friend class NodePoolConfig;

public:
    enum EndpointType
    {
        Ethernet,    //  MAC address specified
        IPv4,   //  IPv4 and port
        IPv6,   //  IPv6 and port
    };

    struct EthernetEndpoint
    {
        __extension__ unsigned __int128 address;
    };

    struct IPv4Endpoint
    {
        uint32_t address;
        uint16_t port;
    };

    struct IPv6Endpoint
    {
        __extension__ unsigned __int128 address;
        uint16_t port;
    };

    EndpointType type;

    union
    {
        EthernetEndpoint ethernet;
        IPv4Endpoint ipv4;
        IPv6Endpoint ipv6;
    };

    std::string _id;

    NodeEndpointConfig()
    {
        // empty
    }
};

} // namespace k2
