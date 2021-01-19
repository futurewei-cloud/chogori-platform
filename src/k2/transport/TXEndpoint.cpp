/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

// stl
#include <regex>

// third-party
#include <seastar/net/rdma.hh>

// k2tx
#include "TXEndpoint.h"
#include "Log.h"

namespace k2 {
// simple regex to help parse
// 1. ipv6 format, e.g. "rdma+k2rpc://[abcd::aabc:23]:1234567"
// 2. or ipv4, e.g. "tcp+k2rpc://1.2.3.4:12345"
// must have: protocol(group1), ip(group2==ipv4, group3==ipv6), port(group4)
const std::regex urlregex{"(.+)://(?:([^:\\[\\]]+)|\\[(.+)\\]):(\\d+)"};

std::unique_ptr<TXEndpoint> TXEndpoint::fromURL(const String& url, BinaryAllocatorFunctor&& allocator) {
    K2LOG_D(log::tx, "Parsing url {}", url);
    std::cmatch matches;
    if (!std::regex_match(url.c_str(), matches, urlregex)) {
        K2LOG_W(log::tx, "Unable to parse url: {}", url);
        return nullptr;
    }
    String protocol(matches[1].str());
    auto isIPV6 = matches[3].length() > 0;
    String ip(matches[isIPV6?3:2].str());
    int64_t parsedport = std::stoll(matches[4].str());
    if (parsedport <0 || parsedport > std::numeric_limits<uint32_t>::max()) {
        K2LOG_W(log::tx, "unable to parse port as int in {}", url);
        return nullptr;
    }
    uint32_t port = (uint32_t) parsedport;

    if (ip.size() == 0) {
        K2LOG_W(log::tx, "unable to find an ip portion in {}", url);
        return nullptr;
    }
    if (protocol.size() == 0) {
        K2LOG_W(log::tx, "unable to find a protocol portion in {}", url);
        return nullptr;
    }
    // convert IP to canonical form
    if (isIPV6) {
        union ibv_gid tmpip6;
        if (seastar::rdma::EndPoint::StringToGID(ip, tmpip6) != 0) {
            K2LOG_W(log::tx, "Invalid ipv6 address: {}", ip);
            return nullptr;
        }
        ip = seastar::rdma::EndPoint::GIDToString(tmpip6);
    }
    return std::make_unique<TXEndpoint>(std::move(protocol), std::move(ip), port, std::move(allocator));
}

TXEndpoint::~TXEndpoint() {
    K2LOG_D(log::tx, "dtor");
}

TXEndpoint::TXEndpoint(String&& pprotocol, String&& pip, uint32_t pport, BinaryAllocatorFunctor&& allocator):
    protocol(std::move(pprotocol)),
    ip(std::move(pip)),
    port(pport),
    _allocator(std::move(allocator)) {
    bool isIpv6 = ip.find(":") != String::npos;
    url = protocol + "://" + (isIpv6?"[":"") + ip + (isIpv6?"]":"");
    url += ":" + std::to_string(port);
    _hash = std::hash<String>()(url);

    K2LOG_D(log::tx, "Created endpoint {}", url);
}

TXEndpoint::TXEndpoint(const TXEndpoint& o) {
    protocol = o.protocol;
    ip = o.ip;
    port = o.port;
    url = o.url;
    _hash = o._hash;
    _allocator = o._allocator;
    K2LOG_D(log::tx, "Copy endpoint {}", url);
}

TXEndpoint::TXEndpoint(TXEndpoint&& o) {
    K2LOG_D(log::tx, "move ctor");
    if (&o == this) {
        K2LOG_D(log::tx, "move ctor on self");
        return;
    }
    K2LOG_D(log::tx, "");
    protocol = std::move(o.protocol);
    ip = std::move(o.ip);
    port = o.port; o.port = 0;
    url = std::move(o.url);
    _hash = o._hash; o._hash = 0;
    _allocator = std::move(o._allocator);
    o._allocator = nullptr;

    K2LOG_D(log::tx, "move ctor done");
}

bool TXEndpoint::operator==(const TXEndpoint& other) const {
    return url == other.url;
}

size_t TXEndpoint::hash() const { return _hash; }

std::unique_ptr<Payload> TXEndpoint::newPayload() {
    K2ASSERT(log::tx, _allocator != nullptr, "asked to create payload from non-allocating endpoint");
    auto result = std::make_unique<Payload>(_allocator);
    // rewind enough bytes to write out a header when we're sending
    result->skip(txconstants::MAX_HEADER_SIZE);
    return result;
}

bool TXEndpoint::canAllocate() const {
    return _allocator != nullptr;
}

}
