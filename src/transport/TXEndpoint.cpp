//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// k2tx
#include "TXEndpoint.h"
#include "common/Log.h"

namespace k2 {

std::unique_ptr<TXEndpoint> TXEndpoint::fromURL(String url, BinaryAllocatorFunctor allocator) {
    K2DEBUG("Parsing url " << url);
    String protocol;
    String ip;
    uint32_t port = 0;
    auto protoend = url.find("://");
    if (protoend == String::npos || protoend == 0) {
        K2WARN("unable to find '://' in " << url);
        return nullptr;
    }
    protocol = url.substr(0, protoend);
    K2DEBUG("found protocol " << protocol);

    // see where the ip ends. start after protoend + 3 since we have protoend+"://"
    auto ipend = url.find(":", protoend + 3); // it's ok if this returns npos - we'll get the rest of the string in ip

    if (ipend != String::npos) {
        ip = url.substr(protoend + 3, ipend-protoend-3);
        K2DEBUG("found ip " << ip);
        // we also have a port
        try {
            int parsedport = std::stoi(url.substr(ipend+1));
            if (parsedport <0) {
                K2WARN("unable to parse port as short int in " << url);
                return nullptr;
            }
            port = parsedport;
            K2DEBUG("found port " << port);
        }
        catch(...) {
            K2WARN("unable to parse url: " << url);
            return nullptr;
        }
    }
    else{
        K2DEBUG("No port found")
        ip = url.substr(protoend + 3);
        K2DEBUG("found ip " << ip);
    }

    if (ip.size() == 0) {
        K2WARN("unable to find an ip portion in " << url);
        return nullptr;
    }

    K2DEBUG("Parsed url " << url << ", into: " << protocol << "://" << ip << ":" << port);
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
    _url = _protocol + "://" + _ip;
    if (_port > 0) {
        _url += ":" + std::to_string(_port);
    }
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
