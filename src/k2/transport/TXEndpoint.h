//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <k2/common/Common.h>
#include "Payload.h"
#include "RPCHeader.h"

namespace k2 {
// an endpoint has: three components: protocol, IP, and port. It can be represented in a string form (url) as
// <protocol>://<ip>:<port>
// NB. we only support case-sensitive URLs, in either domainname, ipv4, or ipv6/rdma
// e.g. domain: http://google.com
//      proto=http, ip=google.com, port=0
// e.g. ipv4: tcp+k2rpc://10.0.0.1:12345
//      proto=tcp+k2rpc, ip=10.0.0.1, port=12345
// e.g. ipv6/rdma: rdma+k2rpc://[2001:db8:85a3::8a2e:370:7334]:1234567
//      proto=rdma+k2rpc, ip=2001:db8:85a3::8a2e:370:7334, port=1234567
class TXEndpoint {

public: // lifecycle
    // construct an endpoint from a url with the given allocator
    // Returns nullptr if there was a problem parsing the url
    static std::unique_ptr<TXEndpoint> fromURL(const String& url, BinaryAllocatorFunctor&& allocator);

    // default ctor
    TXEndpoint() = default;

    // default copy operator=
    TXEndpoint& operator=(const TXEndpoint&) = default;

    // default move operator=
    TXEndpoint& operator=(TXEndpoint&&) = default;

    // construct an endpoint from the tuple (protocol, ip, port) with the given allocator and protocol
    TXEndpoint(String&& protocol, String&& ip, uint32_t port, BinaryAllocatorFunctor&& allocator);

    // copy constructor
    TXEndpoint(const TXEndpoint& o);

    // move constructor
    TXEndpoint(TXEndpoint&& o);

    // destructor
    ~TXEndpoint();

public: // API
    // get the URL for this endpoint
    const String& getURL() const { return _url;}

    // get the protocol for this endpoint
    const String& getProtocol() const { return _protocol;}

    // get the IP for this endpoint
    const String& getIP() const { return _ip;}

    // get the port for this endpoint
    uint32_t getPort() const { return _port;}

    // Comparison. Two endpoints are the same if their hashes are the same
    bool operator==(const TXEndpoint &other) const {
        return _hash == other._hash;
    }

    // the stored hash value for this endpoint.
    size_t hash() const { return _hash; }

    // This method should be used to create new payloads. The payloads are allocated in a manner consistent
    // with the transport for the protocol of this endpoint
    std::unique_ptr<Payload> newPayload() {
        K2ASSERT(_allocator, "asked to create payload from non-allocating endpoint");
        auto result = std::make_unique<Payload>(_allocator, _protocol);
        // rewind enough bytes to write out a header when we're sending
        result->getWriter().skip(txconstants::MAX_HEADER_SIZE);
        return result;
    }

    // Use to determine if this endpoint can allocate
    bool canAllocate() const {
        return _allocator != nullptr;
    }

private: // fields
    String _url;
    String _protocol;
    String _ip;
    uint32_t _port;
    size_t _hash;
    BinaryAllocatorFunctor _allocator;

}; // class TXEndpoint

} // namespace k2

// Implement std::hash for TXEndpoint so that we can use it as key in containers
namespace std {

template <>
struct hash<k2::TXEndpoint> {

size_t operator()(const k2::TXEndpoint& endpoint) const {
    return endpoint.hash();
}

}; // struct hash

} // namespace std
