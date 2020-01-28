#define CATCH_CONFIG_MAIN
// std
#include <k2/transport/Payload.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
// catch
#include "catch2/catch.hpp"
using namespace k2;

struct embeddedSimple {
    int a;
    char b;
    size_t c;
    K2_PAYLOAD_COPYABLE;
    bool operator==(const embeddedSimple& o) const {
        return a == o.a && b == o.b && c == o.c;
    }
};

struct embeddedComplex {
    String a;
    int b;
    char c;
    K2_PAYLOAD_FIELDS(a, b, c);
    bool operator==(const embeddedComplex& o) const {
        return a == o.a && b == o.b && c == o.c;
    }
};

struct data {
    uint32_t a;
    uint64_t b;
    char x;
    embeddedSimple y;
    embeddedComplex z;
    Payload c;
    String d;
    K2_PAYLOAD_FIELDS(a, b, x, y, z, c, d);
    bool operator==(const data& o) const {
        return a == o.a && b==o.b && x == o.x && y==o.y && z==o.z && c==o.c && d==o.d;
    }
};

data makeData(uint32_t a, uint64_t b, char x, int ya, char yb, size_t yc, String za, int zb, char zc, const char* pdata, String d, int allocSize) {
    data result;
    result.a = a;
    result.b = b;
    result.x = x;
    result.y = embeddedSimple{.a=ya, .b=yb, .c=yc};
    result.z = embeddedComplex{.a=std::move(za), .b=zb, .c=zc};
    if (pdata) {
        String pp(pdata);
        result.c = Payload([allocSize]{return Binary(allocSize);});
        result.c.write(pp);
    }
    result.d = std::move(d);
    return result;
}

SCENARIO("test empty payload serialization") {
    Payload src;
    REQUIRE(src.getCurrentPosition().bufferIndex == 0);
    REQUIRE(src.getCurrentPosition().bufferOffset == 0);
    REQUIRE(src.getCurrentPosition().offset == 0);
    REQUIRE(src.getSize() == 0);
    REQUIRE(src.getDataRemaining() == 0);
    REQUIRE(src.getCapacity() == 0);
    REQUIRE(src.computeCrc32c() == 0);

    Payload shared(src.share());
    REQUIRE(shared.getCurrentPosition().bufferIndex == 0);
    REQUIRE(shared.getCurrentPosition().bufferOffset == 0);
    REQUIRE(shared.getCurrentPosition().offset == 0);
    REQUIRE(shared.getSize() == 0);
    REQUIRE(shared.getDataRemaining() == 0);
    REQUIRE(shared.getCapacity() == 0);
    REQUIRE(shared.computeCrc32c() == 0);

    Payload dst([]() { return Binary(999); });
    dst.write(src);
    REQUIRE(dst.getCurrentPosition().bufferIndex == 1);
    REQUIRE(dst.getCurrentPosition().bufferOffset == 0);
    REQUIRE(dst.getCurrentPosition().offset == 8);
    REQUIRE(dst.getSize() == 8);
    REQUIRE(dst.getDataRemaining() == 0);
    REQUIRE(dst.getCapacity() == 8);  // assumes the capacity of src
    REQUIRE(dst.computeCrc32c() == 0);
    dst.seek(0);
    REQUIRE(dst.computeCrc32c() == 2351477386);

    Payload sharedDst(dst.share());
    REQUIRE(sharedDst.getCurrentPosition().bufferIndex == 0);
    REQUIRE(sharedDst.getCurrentPosition().bufferOffset == 0);
    REQUIRE(sharedDst.getCurrentPosition().offset == 0);
    REQUIRE(sharedDst.getSize() == 8);
    REQUIRE(sharedDst.getDataRemaining() == 8);
    REQUIRE(sharedDst.getCapacity() == 8);
    REQUIRE(sharedDst.computeCrc32c() == 2351477386);

    Payload parsed;
    dst.seek(0);
    REQUIRE(dst.read(parsed));
    REQUIRE(parsed.getCurrentPosition().bufferIndex == 0);
    REQUIRE(parsed.getCurrentPosition().bufferOffset == 0);
    REQUIRE(parsed.getCurrentPosition().offset == 0);
    REQUIRE(parsed.getSize() == 0);
    REQUIRE(parsed.getDataRemaining() == 0);
    REQUIRE(parsed.getCapacity() == 0);
    REQUIRE(parsed.computeCrc32c() == 0);
}

SCENARIO("test multi-buffer serialization") {
    Payload dst([]() { return Binary(11); });
    String s(100, 'x');
    dst.write(s);

    String q;
    dst.seek(0);
    REQUIRE(dst.read(q));
    REQUIRE(q.size() == s.size());
    REQUIRE(q == s);

    dst.seek(0);
    Payload dst2([]() { return Binary(23); });
    dst2.write(dst);
    dst2.write(s);
    dst2.write(dst);

    dst2.seek(0);
    dst.seek(0);
    Payload pa,pb;
    String sa;

    REQUIRE(dst2.getCurrentPosition().bufferIndex == 0);
    REQUIRE(dst2.getCurrentPosition().bufferOffset == 0);
    REQUIRE(dst2.getCurrentPosition().offset == 0);
    REQUIRE(dst2.read(pa));
    REQUIRE(dst2.read(sa));
    REQUIRE(dst2.read(pb));
    REQUIRE(sa == s);
    REQUIRE(pa == dst);
    REQUIRE(pb == dst);
    REQUIRE(pa == pb);
}

void checkSize(Payload& p) {
    p.seek(p.getSize());
    p.truncateToCurrent();
    size_t ss = 0;
    auto exp = p.getSize();
    for (auto& b: p.release()) {
        ss += b.size();
    }
    REQUIRE(ss == exp);
}

SCENARIO("test empty payload serialization after some data") {
    std::vector<data> testCases;
    String s(100000, 'x');
    testCases.push_back(makeData(1, 2, 'a', 44, 'f', 123, "hya", 124121123, 's', nullptr, "", 11));
    testCases.push_back(makeData(11,22,'b', 444, 'g', 1231234, "hya", 1241234, 's', "1", "", 101));
    testCases.push_back(makeData(111,2222,'c', 4444, 'h', 12312345, "hya", 1241245, 's', s.c_str(), "", 107));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 124123456, 's', s.c_str(), s, 109));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 1241223456, 's', nullptr, s, 203));
    for (auto& d: testCases) {
        Payload dst([] { return Binary(111); });
        dst.write(d);
        dst.seek(0);
        auto chksum = dst.computeCrc32c();

        data parsed;
        REQUIRE(dst.read(parsed));
        REQUIRE(parsed == d);

        // write the parsed data back and make sure checksum remains the same
        Payload dst2([] { return Binary(110); });
        dst2.write(parsed);
        dst2.seek(0);
        REQUIRE(chksum == dst2.computeCrc32c());

        checkSize(dst);
        checkSize(dst2);
    }
}
