//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
// stl
#include <regex>

// third-party
#include <seastar/net/rdma.hh>

// k2tx
#include "TXEndpoint.h"
#include "common/Log.h"

namespace k2 {
// simple regex to help parse
// 1. ipv6 format, e.g. "rdma+k2rpc://[abcd::aabc:23]:1234567"
// 2. or ipv4, e.g. "tcp+k2rpc://1.2.3.4:12345"
// must have: protocol(group1), ip(group2==ipv4, group3==ipv6), port(group4)
const static std::regex urlregex("(.+)://(?:([^:\\[\\]]+)|\\[(.+)\\]):(\\d+)");

std::unique_ptr<TXEndpoint> TXEndpoint::fromURL(String url, BinaryAllocatorFunctor allocator) {
    K2DEBUG("Parsing url " << url);
    std::cmatch matches;
    if (!std::regex_match(url.c_str(), matches, urlregex)) {
        K2WARN("Unable to parse url: " << url);
        return nullptr;
    }
    String protocol(matches[1].str());
    auto isIPV6 = matches[3].length() > 0;
    String ip(matches[isIPV6?3:2].str());
    int64_t parsedport = std::stoll(matches[4].str());
    if (parsedport <0 || parsedport > std::numeric_limits<uint32_t>::max()) {
        K2WARN("unable to parse port as int in " << url);
        return nullptr;
    }
    uint32_t port = (uint32_t) parsedport;

    if (ip.size() == 0) {
        K2WARN("unable to find an ip portion in " << url);
        return nullptr;
    }
    if (protocol.size() == 0) {
        K2WARN("unable to find a protocol portion in " << url);
        return nullptr;
    }
    // convert IP to canonical form
    if (isIPV6) {
        union ibv_gid tmpip6;
        if (seastar::rdma::EndPoint::StringToGID(ip, tmpip6) != 0) {
            K2WARN("Invalid ipv6 address: " << ip);
            return nullptr;
        }
        ip = seastar::rdma::EndPoint::GIDToString(tmpip6);
    }
    K2DEBUG("Parsed url " << url << ", into: proto=" << protocol << ", ip=" << ip  << ", port=" << port);
    return std::make_unique<TXEndpoint>(protocol, ip, port, std::move(allocator));
}

TXEndpoint::~TXEndpoint() {
    K2DEBUG("dtor");
}

TXEndpoint::TXEndpoint(String protocol, String ip, uint32_t port, BinaryAllocatorFunctor allocator):
    _protocol(std::move(protocol)),
    _ip(std::move(ip)),
    _port(port),
    _allocator(std::move(allocator)) {
    bool isIpv6 = _ip.find(":") != String::npos;
    _url = _protocol + "://" + (isIpv6?"[":"") + _ip + (isIpv6?"]":"");
    _url += ":" + std::to_string(_port);
    _hash = std::hash<String>()(_url);

    K2DEBUG("Created endpoint " << _url);
}

TXEndpoint::TXEndpoint(const TXEndpoint& o) {
    _protocol = o._protocol;
    _ip = o._ip;
    _port = o._port;
    _url = o._url;
    _hash = o._hash;
    _allocator = o._allocator;
    K2DEBUG("Copy endpoint " << _url);
}

TXEndpoint::TXEndpoint(TXEndpoint&& o) {
    K2DEBUG("move ctor");
    if (&o == this) {
        K2DEBUG("move ctor on self");
        return;
    }
    K2DEBUG("");
    _protocol = std::move(o._protocol);
    _ip = std::move(o._ip);
    _port = o._port; o._port = 0;
    _url = std::move(o._url);
    _hash = o._hash; o._hash = 0;
    _allocator = std::move(o._allocator);
    o._allocator = nullptr;

    K2DEBUG("move ctor done");
}

}
