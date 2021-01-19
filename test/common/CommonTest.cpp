/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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
// catch
#include "catch2/catch.hpp"

using namespace k2;

SCENARIO("test encode blank") {
    String s;
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(s == encoded);
    REQUIRE(s == decoded);
}

SCENARIO("test encode printable") {
    String s("a");
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(s == encoded);
    REQUIRE(s == decoded);
}

SCENARIO("test encode special char") {
    String s("^");
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(String("^^") == encoded);
    REQUIRE(s == decoded);
}

SCENARIO("test encode special char with two extra") {
    String s("a^a");
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(String("a^^a") == encoded);
    REQUIRE(s == decoded);
}

SCENARIO("test encode binary null") {
    String s(1, '\0');
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(String("^00") == encoded);
    REQUIRE(s == decoded);
}

SCENARIO("test encode binary") {
    String s(1, '\0');
    s[0] = char((uint8_t) 255);
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(String("^ff") == encoded);
    REQUIRE(s == decoded);
}

SCENARIO("test encode binary with extra and special") {
    String s(4, '\0');
    s[0] = char((uint8_t) 255);
    s[1] = '^';
    s[2] = 'a';
    s[3] = '\0';
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(String("^ff^^a^00") == encoded);
    REQUIRE(s == decoded);
}

SCENARIO("test encode large binary") {
    const size_t sz = 1000;
    String s(sz, '\0');
    for (size_t i = 0; i < sz; ++i) {
        s[i] = char((uint8_t) i);
    }
    String encoded = HexCodec::encode(s);
    String decoded = HexCodec::decode(encoded);
    REQUIRE(s == decoded);
}
