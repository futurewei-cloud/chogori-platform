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

#define CATCH_CONFIG_MAIN
// std
#include <k2/common/Common.h>
#include <k2/transport/TXEndpoint.h>

// catch
#include "catch2/catch.hpp"

using namespace k2;
namespace k2::log {
inline thread_local k2::logging::Logger pt("k2::endpoint_test");
}

SCENARIO("test fromURL bad URLs") {
    std::vector<String> badURLs = {
        "",
        "h",
        "://:",
        "://:123",
        "://123",
        "://123:123",
        "http:",
        "http://",
        "http://[]",
        "http://:123",
        "http://[]:123",
        "http://fw.com",
        "http://[fw.com]",
        "http://fw.com:123",
        "http://[fw.com]:123",
        "http://1.1.1.1:abcd",
        "http://[1::1]:abcd",
        "abc:1.1.1.1",
        "abc::1",
        "abc://[2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF]:abcd",
        "abc://[TTTT:TTTT:TTTT:TTTT:TTTT:TTTT]:123", // bad octets
        "abc://[20001]:123", // bad octets
        "abc://[10101010101010]:123", // bad octets
        "abc://[T]:123", // bad octets
        "abc://[1:1]:123", // bad ipv6 - not enough octets
        "abc://[2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF]", // no port
        "abc://[2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF]:-1", // bad port
        "abc://1.2.3.4", // no port
        "abc://1:1", // bad ipv4
        "abc://a.1.1.1:1", // bad ipv4
        "abc://11111.2222.1.1:1", // bad ipv4
        "abc://1.2.3.4:-1" // bad port
    };

    for (auto url: badURLs) {
        INFO("url: " << url);
        auto x = TXEndpoint::fromURL(url, BinaryAllocator());
        CHECK(!x);
    }
}

SCENARIO("test fromURL good URLs") {
    std::vector<std::tuple<String, String, String, int>> URLs = {
        {"a://1.2.3.4:1", "a", "1.2.3.4", 1},
        // ipv6/rdma
        {"a://[1::1]:1", "a", "1::1", 1},
        {"a://[2001:db8:3333:4444:cccc:dddd:eeee:ffff]:1", "a", "2001:db8:3333:4444:cccc:dddd:eeee:ffff", 1},
        {"a://[2001::ffff]:1", "a", "2001::ffff", 1}
    };

    for (auto&[url, proto, ip, port]: URLs) {
        INFO("url: " << url);
        auto x = TXEndpoint::fromURL(url, BinaryAllocator());
        CHECK(!!x);
        CHECK(x->url == url);
        CHECK(x->protocol == proto);
        CHECK(x->ip == ip);
        CHECK(x->port == port);
    }
}
