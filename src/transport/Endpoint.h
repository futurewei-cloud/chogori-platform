//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include "BaseTypes.h"
#include "Payload.h"
#include "Fragment.h"

namespace k2tx {
// an endpoint has: three components: protocol, IP, and port. It can be represented in a string form (url) as
// <protocol>://<ip>:<port>
// NB. we only support case-sensitive URLs
class Endpoint {

public: // lifecycle
    // construct an endpoint from a url with the given allocator
    // Returns nullptr if there was a problem parsing the url
    static std::unique_ptr<Endpoint> FromURL(String url, Allocator_t allocator);

    // default constructor
    Endpoint();

    // construct an endpoint from the tuple (protocol, ip, port) with the given allocator and protocol
    Endpoint(String protocol, String ip, uint32_t port, Allocator_t allocator);

    // copy constructor
    Endpoint(const Endpoint& o);

    // move constructor
    Endpoint(Endpoint&& o);

    // destructor
    ~Endpoint();

public: // API
    // Get the URL for this endpoint
    const String& GetURL() const { return _url;}

    // Get the protocol for this endpoint
    const String& GetProtocol() const { return _protocol;}

    // Get the IP for this endpoint
    const String& GetIP() const { return _ip;}

    // Get the port for this endpoint
    uint32_t GetPort() const { return _port;}

    // Comparison. Two endpoints are the same if their hashes are the same
    bool operator==(const Endpoint &other) const {
        return _hash == other._hash;
    }

    // the stored hash value for this endpoint.
    size_t Hash() const { return _hash; }

    // This method should be used to create new payloads. The payloads are allocated in a manner consistent
    // with the transport for the protocol of this endpoint
    std::unique_ptr<Payload> NewPayload() {
        if (_allocator) {
            return std::make_unique<Payload>(_allocator, _protocol);
        }
        return nullptr;
    }

    // This method can be used to create a new fragment in a manner consistent
    // with the transport for the protocol of this endpoint
    Fragment NewFragment() {
        return _allocator();
    }

private: // fields
    String _url;
    String _protocol;
    String _ip;
    uint32_t _port;
    size_t _hash;
    Allocator_t _allocator;

private: // Not needed
    Endpoint& operator=(const Endpoint& o) = delete;
    Endpoint& operator=(Endpoint&& o) = delete;

}; // class Endpoint

} // k2tx

// Implement std::hash for Endpoint so that we can use it as key in containers
namespace std {

template <>
struct hash<k2tx::Endpoint> {

size_t operator()(const k2tx::Endpoint& endpoint) const {
    return endpoint.Hash();
}

}; // struct hash

} // namespace std
