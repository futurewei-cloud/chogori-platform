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
#include <k2/transport/Payload.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
// catch
#include "catch2/catch.hpp"

using namespace k2;
namespace k2::log {
inline thread_local k2::logging::Logger pt("k2::payload_test");
}
struct blanks {
    K2_PAYLOAD_EMPTY;
};

struct embeddedSimple {
    int a = 0;
    char b = '\0';
    size_t c = 0;
    K2_PAYLOAD_COPYABLE;
    bool operator==(const embeddedSimple& o) const {
        return a == o.a && b == o.b && c == o.c;
    }
};

struct embeddedComplex {
    String a;
    int b = 0;
    char c = '\0';
    K2_PAYLOAD_FIELDS(a, b, c);
    bool operator==(const embeddedComplex& o) const {
        return a == o.a && b == o.b && c == o.c;
    }
};

template<typename T>
struct data {
    uint32_t a = 0;
    uint64_t b = 0;
    SerializeAsPayload<T> w;
    char x = '\0';
    embeddedSimple y;
    embeddedComplex z;
    Payload c;
    Duration dur{0};
    String d;
    K2_PAYLOAD_FIELDS(a, b, w, x, y, z, c, dur, d);
    bool operator==(const data<T>& o) const {
        return a == o.a && b==o.b && w.val == o.w.val && x == o.x && y==o.y && z==o.z && c==o.c && d==o.d && dur == o.dur;
    }
};

data<embeddedComplex> makeData(uint32_t a, uint64_t b, char x, int ya, char yb, size_t yc, String za, int zb, char zc, const char* pdata, String d, int allocSize, Duration dur) {
    data<embeddedComplex> result;
    result.a = a;
    result.b = b;
    result.w.val = embeddedComplex{.a=za, .b=zb, .c=zc};
    result.x = x;
    result.y = embeddedSimple{.a=ya, .b=yb, .c=yc};
    result.z = embeddedComplex{.a=std::move(za), .b=zb, .c=zc};
    if (pdata) {
        String pp(pdata);
        result.c = Payload(Payload::DefaultAllocator(allocSize));
        result.c.write(pp);
    }
    result.dur = dur;
    result.d = std::move(d);
    return result;
}

SCENARIO("test empty payload serialization") {
    Payload src;
    REQUIRE(src.copy() == src);
    REQUIRE(src.getCurrentPosition().bufferIndex == 0);
    REQUIRE(src.getCurrentPosition().bufferOffset == 0);
    REQUIRE(src.getCurrentPosition().offset == 0);
    REQUIRE(src.getSize() == 0);
    REQUIRE(src.getDataRemaining() == 0);
    REQUIRE(src.getCapacity() == 0);
    REQUIRE(src.computeCrc32c() == 0);

    Payload shared(src.shareAll());
    REQUIRE(shared.getCurrentPosition().bufferIndex == 0);
    REQUIRE(shared.getCurrentPosition().bufferOffset == 0);
    REQUIRE(shared.getCurrentPosition().offset == 0);
    REQUIRE(shared.getSize() == 0);
    REQUIRE(shared.getDataRemaining() == 0);
    REQUIRE(shared.getCapacity() == 0);
    REQUIRE(shared.computeCrc32c() == 0);

    Payload dst(Payload::DefaultAllocator(999));
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

    Payload sharedDst(dst.shareAll());
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
    Payload dst(Payload::DefaultAllocator(11));
    String s(100, 'x');
    dst.write(s);

    String q;
    dst.seek(0);
    REQUIRE(dst.getSize() == 105); // s.size() + 4 bytes for size, 1 byte for '\0'
    REQUIRE(dst.getCapacity() == 110); // 10 binaries allocated at 11b each
    REQUIRE(dst.read(q));
    REQUIRE(q.size() == s.size());
    REQUIRE(q == s);

    dst.seek(0);
    Payload dst2(Payload::DefaultAllocator(23));
    dst2.write(dst);
    REQUIRE(dst2.getSize() == 113); // 8 bytes for size + 105 bytes from dst
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

    Payload p1(Payload::DefaultAllocator(4096));
    Payload p2(Payload::DefaultAllocator(20));
    int32_t a = 10;
    p1.write(a);
    p2.write(p1);
    REQUIRE(p1.getSize() == 4);
    REQUIRE(p1.getCapacity() == 4096);
    REQUIRE(p2.getSize() == 12);
    REQUIRE(p2.getCapacity() == 12);
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

SCENARIO("Serialize/deserialze empty SerializeAsPayload<T>") {
    data<Payload> d{};
    d.a = 100;
    d.b = 200;
    d.x = '!';
    Payload dst(Payload::DefaultAllocator(1500));
    dst.write(d);
    dst.seek(0);

    data<embeddedSimple> parsed;
    REQUIRE(dst.read(parsed));
    REQUIRE(parsed.a == 100);
    REQUIRE(parsed.b == 200);
    REQUIRE(parsed.x == '!');
    {
        // test blank struct read/write
        std::vector<blanks> bvec;
        for (int i = 0; i < 100000; ++i) {
            bvec.push_back(blanks{});
            auto idx = uint64_t(std::rand()) % bvec.size();
            Payload dst(Payload::DefaultAllocator(1500));
            dst.write(bvec[idx]);
            dst.seek(0);
            REQUIRE(dst.computeCrc32c() == 1383945041);
            idx = uint64_t(std::rand()) % bvec.size();
            REQUIRE(dst.read(bvec[idx]));
        }
    }

    {
        // test embedded blank
        for (int i = 0; i < 100; ++i) {
            data<blanks> d{};
            d.a = 1;
            d.b = 2;
            d.x = '!';
            Payload dst(Payload::DefaultAllocator(1500));
            dst.write(d);
            data<Payload> recv;
            dst.seek(0);
            REQUIRE(dst.computeCrc32c() == 4226386812);
            REQUIRE(dst.read(recv));
            REQUIRE(recv.a == 1);
            REQUIRE(recv.b == 2);
            REQUIRE(recv.x == '!');
        }
    }
}

SCENARIO("test empty payload serialization after some data") {
    std::vector<data<embeddedComplex>> testCases;
    String s(100000, 'x');
    testCases.push_back(makeData(1, 2, 'a', 44, 'f', 123, "hya", 124121123, 's', nullptr, "", 11, Duration(10ms)));
    testCases.push_back(makeData(11,22,'b', 444, 'g', 1231234, "hya", 1241234, 's', "1", "", 101, Duration(11us)));
    testCases.push_back(makeData(111,2222,'c', 4444, 'h', 12312345, "hya", 1241245, 's', s.c_str(), "", 107, Duration(13ns)));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 124123456, 's', s.c_str(), s, 109, Duration(21s)));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 1241223456, 's', nullptr, s, 203, Duration(123456789s)));
    for (auto& d: testCases) {
        Payload dst(Payload::DefaultAllocator(111));
        dst.write(d);
        dst.seek(0);
        auto chksum = dst.computeCrc32c();

        data<embeddedComplex> parsed;
        REQUIRE(dst.read(parsed));
        K2LOG_I(log::pt, "{}", parsed.dur);
        K2LOG_I(log::pt, "{}", d.dur);
        REQUIRE(parsed == d);

        // try same with embedded type of Payload
        dst.seek(0);
        data<Payload> parsedAsPayload;
        REQUIRE(dst.read(parsedAsPayload));
        REQUIRE((parsedAsPayload.a == d.a && parsedAsPayload.b == d.b && parsedAsPayload.x == d.x && parsedAsPayload.y == d.y && parsedAsPayload.z == d.z && parsedAsPayload.c == d.c && parsedAsPayload.d == d.d));
        embeddedComplex cmplx;
        REQUIRE(parsedAsPayload.w.val.read(cmplx));
        REQUIRE(cmplx == d.w.val);

        // write the parsed data back and make sure checksum remains the same
        Payload dst2(Payload::DefaultAllocator(110));
        dst2.write(parsed);
        dst2.seek(0);
        REQUIRE(chksum == dst2.computeCrc32c());

        checkSize(dst);
        checkSize(dst2);
        REQUIRE(dst.copy() == dst);
        REQUIRE(dst2.copy() == dst2);
    }
}

SCENARIO("test copy from payload") {
    std::vector<data<embeddedComplex>> testCases;
    String s(100000, 'x');
    testCases.push_back(makeData(1, 2, 'a', 44, 'f', 123, "hya", 124121123, 's', nullptr, "", 11, Duration(10ms)));
    testCases.push_back(makeData(11,22,'b', 444, 'g', 1231234, "hya", 1241234, 's', "1", "", 101, Duration(11us)));
    testCases.push_back(makeData(111,2222,'c', 4444, 'h', 12312345, "hya", 1241245, 's', s.c_str(), "", 107, Duration(13ns)));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 124123456, 's', s.c_str(), s, 109, Duration(21s)));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 1241223456, 's', nullptr, s, 203, Duration(123456789s)));
    for (auto& d: testCases) {
        Payload dst(Payload::DefaultAllocator(111));
        dst.write(d);
        dst.seek(0);
        auto chksum = dst.computeCrc32c();

        Payload dst2(Payload::DefaultAllocator(111));
        dst2.copyFromPayload(dst, dst.getSize());
        dst2.seek(0);

        REQUIRE(chksum == dst2.computeCrc32c());

        checkSize(dst);
        checkSize(dst2);
        REQUIRE(dst.copy() == dst);
        REQUIRE(dst2.copy() == dst2);
    }
}

SCENARIO("test shareAll() and shareRegion()") {
    std::vector<data<embeddedComplex>> testCases;
    String s(100000, 'x');
    testCases.push_back(makeData(1, 2, 'a', 44, 'f', 123, "hya", 124121123, 's', nullptr, "", 11, Duration(10ms)));
    testCases.push_back(makeData(11,22,'b', 444, 'g', 1231234, "hya", 1241234, 's', "1", "", 101, Duration(11us)));
    testCases.push_back(makeData(111,2222,'c', 4444, 'h', 12312345, "hya", 1241245, 's', s.c_str(), "", 107, Duration(13ns)));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 124123456, 's', s.c_str(), s, 109, Duration(21s)));
    testCases.push_back(makeData(1111, 22222, 'd', 44444, 'i', 123123456, s, 1241223456, 's', nullptr, s, 203, Duration(123456789s)));

    Payload dst(Payload::DefaultAllocator(4096));
    std::vector<uint32_t> offsetCheckPoint;
    for (auto& d: testCases) {
        offsetCheckPoint.push_back(dst.getSize());
        dst.write(d);
    }
    offsetCheckPoint.push_back(dst.getSize());

    Payload sharedDst = dst.shareAll();
    sharedDst.seek(0);
    for (auto& d: testCases) {
        data<Payload> parsedAsPayload;
        REQUIRE(sharedDst.read(parsedAsPayload));
        REQUIRE((parsedAsPayload.a == d.a && parsedAsPayload.b == d.b && parsedAsPayload.x == d.x && parsedAsPayload.y == d.y && parsedAsPayload.z == d.z && parsedAsPayload.c == d.c && parsedAsPayload.d == d.d));
        embeddedComplex cmplx;
        REQUIRE(parsedAsPayload.w.val.read(cmplx));
        REQUIRE(cmplx == d.w.val);
    }

    for (long unsigned int i=0; i < testCases.size(); ++i)
        for (long unsigned int j=i; j < testCases.size(); ++j){
            auto offset = offsetCheckPoint[i];
            auto size = offsetCheckPoint[j+1] - offsetCheckPoint[i];
            Payload sharedDst = dst.shareRegion(offset, size);
            sharedDst.seek(0);

            for (long unsigned int k=i; k <= j; ++k){
                auto& d = testCases[k];
                data<Payload> parsedAsPayload;
                REQUIRE(sharedDst.read(parsedAsPayload));
                REQUIRE((parsedAsPayload.a == d.a && parsedAsPayload.b == d.b && parsedAsPayload.x == d.x && parsedAsPayload.y == d.y && parsedAsPayload.z == d.z && parsedAsPayload.c == d.c && parsedAsPayload.d == d.d));
                embeddedComplex cmplx;
                REQUIRE(parsedAsPayload.w.val.read(cmplx));
                REQUIRE(cmplx == d.w.val);
            }
        }
}

SCENARIO("test getSerializedSizeOf method") {
    Payload src(Payload::DefaultAllocator(32));
    char a = 'a';
    String b = "test getSerializedSizeOf method";
    boost::multiprecision::cpp_dec_float_50 c("1333666666.0000001111");
    boost::multiprecision::cpp_dec_float_100 c1("1333666666.00000011114444");
    std::set<int16_t> d{
        1, 2, 3, 4, 5
    };
    std::map<int16_t, String> e{
            {1, "test"},
            {2, "getSerializedSizeOf"},
            {3, "method"}
    };
    SerializeAsPayload<String> f{
        "test getSerializedSizeOf method"
    };
    embeddedSimple g{
        16,
        'g',
        111
    };
    embeddedComplex h{
        "test getSerializedSizeOf method",
        213,
        'h'
    };
    std::unordered_map<int16_t, std::vector<int16_t>> i{
            {1, {1}},
            {2, {2, 3}},
            {3, {3, 4, 5}},
    };
    Payload j(Payload::DefaultAllocator(32));
    j.write(b);

    src.write(a);
    src.write(b);
    src.write(c);
    src.write(c1);
    src.write(d);
    src.write(e);
    src.write(f);
    src.write(g);
    src.write(h);
    src.write(i);
    src.write(j);
    src.seek(0);
    REQUIRE(src.getSerializedSizeOf<char>() == sizeof(char));
    src.skip<char>();
    REQUIRE(src.getSerializedSizeOf<String>() == sizeof(uint32_t) + b.size() + 1);
    src.skip<String>();
    REQUIRE(src.getSerializedSizeOf<boost::multiprecision::cpp_dec_float_50>() == sizeof(size_t) + c.str().size() + 1);
    src.skip<boost::multiprecision::cpp_dec_float_50>();
    REQUIRE(src.getSerializedSizeOf<boost::multiprecision::cpp_dec_float_100>() == sizeof(size_t) + c1.str().size() + 1);
    src.skip<boost::multiprecision::cpp_dec_float_100>();
    REQUIRE(src.getSerializedSizeOf<std::set<int16_t>>() == sizeof(uint32_t) + d.size() * sizeof(int16_t));
    src.skip<std::set<int16_t>>();
    REQUIRE(src.getSerializedSizeOf<std::map<int16_t, String>>() == sizeof(uint32_t) + 2 + 9 + 2 + 24 + 2 + 11);
    src.skip<std::map<int16_t, String>>();
    REQUIRE(src.getSerializedSizeOf<SerializeAsPayload<String>>() == sizeof(uint64_t) + 36);
    src.skip<SerializeAsPayload<String>>();
    REQUIRE(src.getSerializedSizeOf<embeddedSimple>() == sizeof(embeddedSimple));
    src.skip<embeddedSimple>();
    REQUIRE(src.getSerializedSizeOf<embeddedComplex>() == 36 + sizeof(int) + sizeof(char));
    src.skip<embeddedComplex>();
    REQUIRE(src.getSerializedSizeOf<std::unordered_map<int16_t, std::vector<int16_t>>>()
            == sizeof(uint32_t)
                + sizeof(int16_t) + sizeof(uint32_t) + sizeof(int16_t)
                + sizeof(int16_t) + sizeof(uint32_t) + 2 * sizeof(int16_t)
                + sizeof(int16_t) + sizeof(uint32_t) + 3 * sizeof(int16_t));
    src.skip<std::unordered_map<int16_t, std::vector<int16_t>>>();
    REQUIRE(src.getSerializedSizeOf<Payload>() == sizeof(size_t) + j.getSize());
    src.skip<Payload>();
    REQUIRE(src.getDataRemaining() == 0);
}


/*
SCENARIO("rpc parsing") {
    RPCParser([] { return false; }, false) parseNoCRC;
    RPCParser([] { return false; }, true) parseCRC;
}
*/
