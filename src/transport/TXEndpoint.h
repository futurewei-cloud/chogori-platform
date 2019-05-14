//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include "common/Common.h"
#include "common/Payload.h"

namespace k2 {
// an endpoint has: three components: protocol, IP, and port. It can be represented in a string form (url) as
// <protocol>://<ip>:<port>
// NB. we only support case-sensitive URLs
class TXEndpoint {

public: // lifecycle
    // construct an endpoint from a url with the given allocator
    // Returns nullptr if there was a problem parsing the url
    static std::unique_ptr<TXEndpoint> FromURL(String url, BinaryAllocatorFunctor allocator);

    // default constructor
    TXEndpoint();

    // construct an endpoint from the tuple (protocol, ip, port) with the given allocator and protocol
    TXEndpoint(String protocol, String ip, uint32_t port, BinaryAllocatorFunctor allocator);

    // copy constructor
    TXEndpoint(const TXEndpoint& o);

    // move constructor
    TXEndpoint(TXEndpoint&& o);
\
    // destructor
    ~TXEndpoint();

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
    bool operator==(const TXEndpoint &other) const {
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
        throw Payload::NonAllocatingPayloadException();
    }

    // This method can be used to create a new binary in a manner consistent
    // with the transport for the protocol of this endpoint
    Binary NewBinary() {
        if (_allocator) {
            return _allocator();
        }
        throw Payload::NonAllocatingPayloadException();
    }

    // Use to determine if this endpoint can allocate
    bool CanAllocate() const {
        return _allocator != nullptr;
    }

private: // fields
    String _url;
    String _protocol;
    String _ip;
    uint32_t _port;
    size_t _hash;
    BinaryAllocatorFunctor _allocator;

private: // Not needed
    TXEndpoint& operator=(const TXEndpoint& o) = delete;
    TXEndpoint& operator=(TXEndpoint&& o) = delete;

}; // class TXEndpoint

} // namespace k2

// Implement std::hash for TXEndpoint so that we can use it as key in containers
namespace std {

template <>
struct hash<k2::TXEndpoint> {

size_t operator()(const k2::TXEndpoint& endpoint) const {
    return endpoint.Hash();
}

}; // struct hash

} // namespace std
