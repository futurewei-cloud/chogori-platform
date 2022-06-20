/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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
#include <skvhttp/mpack/MPackSerialization.h>

#include <iostream>
#include <string>

#define CATCH_CONFIG_MAIN
#include <catch.hpp>

SCENARIO("Test 01: test buffer packer/unpacker serialization") {
    k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevel::Verbose;
    skv::http::Binary buf1;
    {
        skv::http::MPackWriter w;
        w.write(10);
        w.write(skv::http::String("abcd"));
        std::optional<int> o1;
        std::optional<int> o2 = std::make_optional<int>(20);
        w.write(o1);
        w.write(o2);
        auto fr = w.flush(buf1);
        REQUIRE(fr);
    }
    skv::http::MPackReader r(buf1);
    int x = 0;
    skv::http::String s;

    REQUIRE(r.read(x));
    REQUIRE(x == 10);

    REQUIRE(r.read(s));
    REQUIRE(s == "abcd");

    std::optional<int> o1, o2;
    REQUIRE(r.read(o1));
    REQUIRE(!o1.has_value());

    REQUIRE(r.read(o2));
    REQUIRE(*o2 == 20);
}

struct Ex1 {
    K2_SERIALIZABLE_FMT(Ex1);
};
struct Ex2 {
    int a;
    skv::http::String b;
    std::vector<skv::http::String> b2;
    std::vector<bool> b3;
    std::vector<int> b4;
    std::tuple<int, int> b5;
    Ex1 c;
    K2_SERIALIZABLE_FMT(Ex2, a, b, b2, b3, b4, b5, c);
};

struct Ex4 { // trivial struct
    int a;
    int64_t b;
    uint8_t c;
    K2_SERIALIZABLE_FMT(Ex4, a, b, c);
};


struct Ex3 {
    std::vector<Ex2> a;
    std::list<Ex2> a1;
    std::deque<Ex2> a2;
    std::map<skv::http::String, Ex2> b;
    std::unordered_map<skv::http::String, Ex2> b1;
    std::set<skv::http::String> b2;
    std::unordered_set<skv::http::String> b3;
    std::tuple<int, Ex1, Ex2, skv::http::String, std::map<int, Ex2>> b4;
    skv::http::Duration b5;
    uint8_t c;
    int8_t d;
    uint16_t e;
    int16_t f;
    uint32_t g;
    int32_t h;
    uint64_t i;
    int64_t j;
    skv::http::Decimal64 k;
    skv::http::Decimal128 l;
    skv::http::Binary m;
    Ex4 e4;
    K2_DEF_ENUM_IC(Action, A1, A2);
    Action n;
    std::optional<int> o;
    std::optional<skv::http::Duration> p;
    K2_SERIALIZABLE_FMT(Ex3, a, a1, a2, b, b1, b2, b3, b4, b5, c, d, e, f, g, h, i, j, k, l, m, e4, n, o, p);
};

SCENARIO("Test 02: test struct") {
    skv::http::Binary buf;
    {
        Ex1 ex{};
        skv::http::MPackWriter w;
        w.write(ex);
        REQUIRE(w.flush(buf));
    }
    {
        Ex1 ex{};
        skv::http::MPackReader reader(buf);
        REQUIRE(reader.read(ex));
        // check to make sure we can compile when formatting the struct
        K2LOG_I(skv::http::log::mpack, "read {}", ex);
    }
}

SCENARIO("Test 03: test simple embedded struct") {
    skv::http::Binary buf;
    {
        Ex2 ex{};
        skv::http::MPackWriter w;
        w.write(ex);
        REQUIRE(w.flush(buf));
    }
    {
        Ex2 ex{};
        skv::http::MPackReader reader(buf);
        REQUIRE(reader.read(ex));
        // check to make sure we can compile when formatting the struct
        K2LOG_I(skv::http::log::mpack, "read {}", ex);
    }
}

SCENARIO("Test 04: test complex embedded struct") {
    skv::http::Binary buf;
    {
        Ex3 ex{};
        skv::http::MPackWriter w;
        w.write(ex);
        REQUIRE(w.flush(buf));
    }
    {
        Ex3 ex{};
        skv::http::MPackReader reader(buf);
        REQUIRE(reader.read(ex));
        // check to make sure we can compile when formatting the struct
        K2LOG_I(skv::http::log::mpack, "read {}", ex);
    }
}

SCENARIO("Test 05: test complex embedded struct with data") {
    skv::http::Binary buf;
    Ex3 orig{
        .a={{.a=10, .b="001",.b2={"002", "003"}, .b3={0,1,1,0}, .b4={1,2,3,4}, .b5={20,30}, .c={}},
            {.a=40, .b="004",.b2={"005", "006"}, .b3={1,1,1,0}, .b4={5,6,7,8}, .b5={40,50}, .c={}}},
        .a1={{.a=11, .b="0011",.b2={"0021", "0031"}, .b3={0,1,1,1}, .b4={1,2,3,1}, .b5={201,301}, .c={}},
            {.a=41, .b="0041",.b2={"0051", "0061"}, .b3={1,1,1,1}, .b4={5,6,7,1}, .b5={401,501}, .c={}}},
        .a2={{.a=102, .b="0012",.b2={"0022", "0032"}, .b3={0,1,1,0,1}, .b4={1,2,3,4,5}, .b5={202,302}, .c={}},
            {.a=402, .b="0042",.b2={"0052", "0062"}, .b3={1,1,1,0,1}, .b4={5,6,7,8,9}, .b5={402,502}, .c={}}},
        .b={{"el1",{.a=1021, .b="10421",.b2={"10521", "10621"}, .b3={1,0,1,1}, .b4={7,8,9,4}, .b5={1402,1505}, .c={}}},
            {"el2",{.a=1022, .b="10422",.b2={"10522", "10622"}, .b3={1,0,1,1,0}, .b4={7,8,9,2}, .b5={122,052}, .c={}}}},
        .b1={{"el3",{.a=3021, .b="10423",.b2={"10523", "10623"}, .b3={1,0,1,1,0}, .b4={7,8,9,4,3}, .b5={143,153}, .c={}}},
            {"el4",{.a=14, .b="104",.b2={"14", "1042"}, .b3={1,0,0}, .b4={7,8,4}, .b5={124,054}, .c={}}}},
        .b2={"k1", "k2", "k3"},
        .b3={"k4", "k5", "k6"},
        .b4={41,
                {},
                {.a=4021, .b="00421",.b2={"00521", "00621"}, .b3={1,1,1,0,1,1}, .b4={5,6,7,8,9,4}, .b5={402,505}, .c={}},
                "something",
                {
                {0,{.a=31, .b="103",.b2={"103", "123"}, .b3={1,1,0}, .b4={7,4,3}, .b5={43,53}, .c={}}},
                {2,{.a=14, .b="104",.b2={"149", "102"}, .b3={1,0,0,1}, .b4={7,8,4, 1}, .b5={24,54}, .c={}}}
                }},
        .b5=20ms,
        .c=90,
        .d=91,
        .e=92,
        .f=93,
        .g=94,
        .h=95,
        .i=96,
        .j=99,
        .k=50,
        .l=100,
        .m=skv::http::Binary(skv::http::String("abcd")),
        .e4={5, 4, 3},
        .n=Ex3::Action::A2,
        .o={},
        .p=std::make_optional<skv::http::Duration>(30ns)
    };
    {
        skv::http::MPackWriter w;
        w.write(orig);
        REQUIRE(w.flush(buf));
    }
    {
        Ex3 ex{};
        skv::http::MPackReader reader(buf);
        REQUIRE(reader.read(ex));

        // check to make sure we can compile when formatting the struct
        K2LOG_I(skv::http::log::mpack, "read {}", ex);
        REQUIRE(ex.a.size() == 2);
        REQUIRE(ex.a[0].a == 10);
        REQUIRE(ex.a[0].b == "001");
        REQUIRE(ex.a[0].b2.size() == 2);
        REQUIRE(ex.a[0].b2[0] == "002");
        REQUIRE(ex.a[0].b2[1] == "003");
        REQUIRE(ex.a[0].b3.size() == 4);
        REQUIRE(ex.a[0].b3[0] == 0);
        REQUIRE(ex.a[0].b3[1] == 1);
        REQUIRE(ex.a[0].b3[2] == 1);
        REQUIRE(ex.a[0].b3[3] == 0);
        REQUIRE(ex.a[0].b4.size() == 4);
        REQUIRE(ex.a[0].b4[0] == 1);
        REQUIRE(ex.a[0].b4[1] == 2);
        REQUIRE(ex.a[0].b4[2] == 3);
        REQUIRE(ex.a[0].b4[3] == 4);
        REQUIRE(ex.a[0].b5 == std::tuple<int, int>(20,30));

        REQUIRE(ex.a[1].a == 40);
        REQUIRE(ex.a[1].b == "004");
        REQUIRE(ex.a[1].b2.size() == 2);
        REQUIRE(ex.a[1].b2[0] == "005");
        REQUIRE(ex.a[1].b2[1] == "006");
        REQUIRE(ex.a[1].b3.size() == 4);
        REQUIRE(ex.a[1].b3[0] == 1);
        REQUIRE(ex.a[1].b3[1] == 1);
        REQUIRE(ex.a[1].b3[2] == 1);
        REQUIRE(ex.a[1].b3[3] == 0);
        REQUIRE(ex.a[1].b4.size() == 4);
        REQUIRE(ex.a[1].b4[0] == 5);
        REQUIRE(ex.a[1].b4[1] == 6);
        REQUIRE(ex.a[1].b4[2] == 7);
        REQUIRE(ex.a[1].b4[3] == 8);
        REQUIRE(ex.a[1].b5 == std::tuple<int, int>(40, 50));

        // check to make sure we can compile when formatting the struct
        K2LOG_I(skv::http::log::mpack, "read {}", ex);
        REQUIRE(ex.a1.size() == 2);
        REQUIRE(ex.a1.front().a == 11);
        REQUIRE(ex.a1.front().b == "0011");
        REQUIRE(ex.a1.front().b2.size() == 2);
        REQUIRE(ex.a1.front().b2[0] == "0021");
        REQUIRE(ex.a1.front().b2[1] == "0031");
        REQUIRE(ex.a1.front().b3.size() == 4);
        REQUIRE(ex.a1.front().b3[0] == 0);
        REQUIRE(ex.a1.front().b3[1] == 1);
        REQUIRE(ex.a1.front().b3[2] == 1);
        REQUIRE(ex.a1.front().b3[3] == 1);
        REQUIRE(ex.a1.front().b4.size() == 4);
        REQUIRE(ex.a1.front().b4[0] == 1);
        REQUIRE(ex.a1.front().b4[1] == 2);
        REQUIRE(ex.a1.front().b4[2] == 3);
        REQUIRE(ex.a1.front().b4[3] == 1);
        REQUIRE(ex.a1.front().b5 == std::tuple<int, int>(201, 301));

        REQUIRE(ex.a1.back().a == 41);
        REQUIRE(ex.a1.back().b == "0041");
        REQUIRE(ex.a1.back().b2.size() == 2);
        REQUIRE(ex.a1.back().b2[0] == "0051");
        REQUIRE(ex.a1.back().b2[1] == "0061");
        REQUIRE(ex.a1.back().b3.size() == 4);
        REQUIRE(ex.a1.back().b3[0] == 1);
        REQUIRE(ex.a1.back().b3[1] == 1);
        REQUIRE(ex.a1.back().b3[2] == 1);
        REQUIRE(ex.a1.back().b3[3] == 1);
        REQUIRE(ex.a1.back().b4.size() == 4);
        REQUIRE(ex.a1.back().b4[0] == 5);
        REQUIRE(ex.a1.back().b4[1] == 6);
        REQUIRE(ex.a1.back().b4[2] == 7);
        REQUIRE(ex.a1.back().b4[3] == 1);
        REQUIRE(ex.a1.back().b5 == std::tuple<int, int>(401, 501));

        REQUIRE(ex.a2.size() == 2);
        REQUIRE(ex.a2[0].a == 102);
        REQUIRE(ex.a2[0].b == "0012");
        REQUIRE(ex.a2[0].b2.size() == 2);
        REQUIRE(ex.a2[0].b2[0] == "0022");
        REQUIRE(ex.a2[0].b2[1] == "0032");
        REQUIRE(ex.a2[0].b3.size() == 5);
        REQUIRE(ex.a2[0].b3[0] == 0);
        REQUIRE(ex.a2[0].b3[1] == 1);
        REQUIRE(ex.a2[0].b3[2] == 1);
        REQUIRE(ex.a2[0].b3[3] == 0);
        REQUIRE(ex.a2[0].b3[4] == 1);
        REQUIRE(ex.a2[0].b4.size() == 5);
        REQUIRE(ex.a2[0].b4[0] == 1);
        REQUIRE(ex.a2[0].b4[1] == 2);
        REQUIRE(ex.a2[0].b4[2] == 3);
        REQUIRE(ex.a2[0].b4[3] == 4);
        REQUIRE(ex.a2[0].b4[4] == 5);
        REQUIRE(ex.a2[0].b5 == std::tuple<int, int>(202, 302));

        REQUIRE(ex.a2[1].a == 402);
        REQUIRE(ex.a2[1].b == "0042");
        REQUIRE(ex.a2[1].b2.size() == 2);
        REQUIRE(ex.a2[1].b2[0] == "0052");
        REQUIRE(ex.a2[1].b2[1] == "0062");
        REQUIRE(ex.a2[1].b3.size() == 5);
        REQUIRE(ex.a2[1].b3[0] == 1);
        REQUIRE(ex.a2[1].b3[1] == 1);
        REQUIRE(ex.a2[1].b3[2] == 1);
        REQUIRE(ex.a2[1].b3[3] == 0);
        REQUIRE(ex.a2[1].b3[4] == 1);
        REQUIRE(ex.a2[1].b4.size() == 5);
        REQUIRE(ex.a2[1].b4[0] == 5);
        REQUIRE(ex.a2[1].b4[1] == 6);
        REQUIRE(ex.a2[1].b4[2] == 7);
        REQUIRE(ex.a2[1].b4[3] == 8);
        REQUIRE(ex.a2[1].b4[4] == 9);
        REQUIRE(ex.a2[1].b5 == std::tuple<int, int>(402, 502));

        REQUIRE(ex.b.size() == 2);
        REQUIRE(ex.b["el1"].a == 1021);
        REQUIRE(ex.b["el1"].b == "10421");
        REQUIRE(ex.b["el1"].b2 == std::vector<skv::http::String>({"10521", "10621"}));
        REQUIRE(ex.b["el1"].b3 == std::vector<bool>({1, 0, 1, 1}));
        REQUIRE(ex.b["el1"].b4 == std::vector<int>({7, 8, 9, 4}));
        REQUIRE(ex.b["el1"].b5 == std::tuple<int, int>(1402, 1505));
        REQUIRE(ex.b["el2"].a == 1022);
        REQUIRE(ex.b["el2"].b == "10422");
        REQUIRE(ex.b["el2"].b2 == std::vector<skv::http::String>({"10522", "10622"}));
        REQUIRE(ex.b["el2"].b3 == std::vector<bool>({1, 0, 1, 1, 0}));
        REQUIRE(ex.b["el2"].b4 == std::vector<int>({7, 8, 9, 2}));
        REQUIRE(ex.b["el2"].b5 == std::tuple<int, int>(122, 052));

        REQUIRE(ex.b1["el3"].a == 3021);
        REQUIRE(ex.b1["el3"].b == "10423");
        REQUIRE(ex.b1["el3"].b2 == std::vector<skv::http::String>({"10523", "10623"}));
        REQUIRE(ex.b1["el3"].b3 == std::vector<bool>({1, 0, 1, 1, 0}));
        REQUIRE(ex.b1["el3"].b4 == std::vector<int>({7, 8, 9, 4, 3}));
        REQUIRE(ex.b1["el3"].b5 == std::tuple<int, int>(143, 153));
        REQUIRE(ex.b1["el4"].a == 14);
        REQUIRE(ex.b1["el4"].b == "104");
        REQUIRE(ex.b1["el4"].b2 == std::vector<skv::http::String>({"14", "1042"}));
        REQUIRE(ex.b1["el4"].b3 == std::vector<bool>({1, 0, 0}));
        REQUIRE(ex.b1["el4"].b4 == std::vector<int>({7, 8, 4}));
        REQUIRE(ex.b1["el4"].b5 == std::tuple<int, int>(124, 054));

        REQUIRE(ex.b2.size() == 3);
        REQUIRE(ex.b2.count("k1") == 1);
        REQUIRE(ex.b2.count("k2") == 1);
        REQUIRE(ex.b2.count("k3") == 1);
        REQUIRE(ex.b3.size() == 3);
        REQUIRE(ex.b3.count("k4") == 1);
        REQUIRE(ex.b3.count("k5") == 1);
        REQUIRE(ex.b3.count("k6") == 1);

        REQUIRE(std::get<0>(ex.b4) == 41);
        REQUIRE(std::get<2>(ex.b4).a == 4021);
        REQUIRE(std::get<2>(ex.b4).b == "00421");

        REQUIRE(std::get<2>(ex.b4).b2.size() == 2);
        REQUIRE(std::get<2>(ex.b4).b2[0] == "00521");
        REQUIRE(std::get<2>(ex.b4).b2[1] == "00621");

        REQUIRE(std::get<2>(ex.b4).b3.size() == 6);
        REQUIRE(std::get<2>(ex.b4).b3[0] == 1);
        REQUIRE(std::get<2>(ex.b4).b3[1] == 1);
        REQUIRE(std::get<2>(ex.b4).b3[2] == 1);
        REQUIRE(std::get<2>(ex.b4).b3[3] == 0);
        REQUIRE(std::get<2>(ex.b4).b3[4] == 1);
        REQUIRE(std::get<2>(ex.b4).b3[5] == 1);

        REQUIRE(std::get<2>(ex.b4).b4.size() == 6);
        REQUIRE(std::get<2>(ex.b4).b4[0] == 5);
        REQUIRE(std::get<2>(ex.b4).b4[1] == 6);
        REQUIRE(std::get<2>(ex.b4).b4[2] == 7);
        REQUIRE(std::get<2>(ex.b4).b4[3] == 8);
        REQUIRE(std::get<2>(ex.b4).b4[4] == 9);
        REQUIRE(std::get<2>(ex.b4).b4[5] == 4);

        REQUIRE(std::get<2>(ex.b4).b5 == std::tuple<int,int>(402,505));

        REQUIRE(std::get<3>(ex.b4) == "something");

        REQUIRE(std::get<4>(ex.b4).size() == 2);
        REQUIRE(std::get<4>(ex.b4)[0].a == 31);
        REQUIRE(std::get<4>(ex.b4)[0].b == "103");
        REQUIRE(std::get<4>(ex.b4)[0].b2.size() == 2);
        REQUIRE(std::get<4>(ex.b4)[0].b2[0] == "103");
        REQUIRE(std::get<4>(ex.b4)[0].b2[1] == "123");
        REQUIRE(std::get<4>(ex.b4)[0].b3.size() == 3);
        REQUIRE(std::get<4>(ex.b4)[0].b3[0] == 1);
        REQUIRE(std::get<4>(ex.b4)[0].b3[1] == 1);
        REQUIRE(std::get<4>(ex.b4)[0].b3[2] == 0);
        REQUIRE(std::get<4>(ex.b4)[0].b4.size() == 3);
        REQUIRE(std::get<4>(ex.b4)[0].b4[0] == 7);
        REQUIRE(std::get<4>(ex.b4)[0].b4[1] == 4);
        REQUIRE(std::get<4>(ex.b4)[0].b4[2] == 3);
        REQUIRE(std::get<4>(ex.b4)[0].b5 == std::tuple<int, int>(43, 53));

        REQUIRE(std::get<4>(ex.b4)[2].a == 14);
        REQUIRE(std::get<4>(ex.b4)[2].b == "104");
        REQUIRE(std::get<4>(ex.b4)[2].b2.size() == 2);
        REQUIRE(std::get<4>(ex.b4)[2].b2[0] == "149");
        REQUIRE(std::get<4>(ex.b4)[2].b2[1] == "102");
        REQUIRE(std::get<4>(ex.b4)[2].b3.size() == 4);
        REQUIRE(std::get<4>(ex.b4)[2].b3[0] == 1);
        REQUIRE(std::get<4>(ex.b4)[2].b3[1] == 0);
        REQUIRE(std::get<4>(ex.b4)[2].b3[2] == 0);
        REQUIRE(std::get<4>(ex.b4)[2].b3[3] == 1);
        REQUIRE(std::get<4>(ex.b4)[2].b4.size() == 4);
        REQUIRE(std::get<4>(ex.b4)[2].b4[0] == 7);
        REQUIRE(std::get<4>(ex.b4)[2].b4[1] == 8);
        REQUIRE(std::get<4>(ex.b4)[2].b4[2] == 4);
        REQUIRE(std::get<4>(ex.b4)[2].b4[3] == 1);
        REQUIRE(std::get<4>(ex.b4)[2].b5 == std::tuple<int, int>(24, 54));


        REQUIRE(ex.b5 == 20ms);
        REQUIRE(ex.c == 90);
        REQUIRE(ex.d == 91);
        REQUIRE(ex.e == 92);
        REQUIRE(ex.f == 93);
        REQUIRE(ex.g == 94);
        REQUIRE(ex.h == 95);
        REQUIRE(ex.i == 96);
        REQUIRE(ex.j == 99);
        REQUIRE(ex.k == 50);
        REQUIRE(ex.l == 100);
        REQUIRE(ex.m.size() == 4);
        REQUIRE(ex.m.data()[0] == 'a');
        REQUIRE(ex.m.data()[1] == 'b');
        REQUIRE(ex.m.data()[2] == 'c');
        REQUIRE(ex.m.data()[3] == 'd');
        REQUIRE(skv::http::String(ex.m.data(), ex.m.size()) == "abcd");

        REQUIRE(ex.e4.a == 5);
        REQUIRE(ex.e4.b == 4);
        REQUIRE(ex.e4.c == 3);

        REQUIRE(ex.n == Ex3::Action::A2);
        REQUIRE(!ex.o.has_value());
        REQUIRE(ex.p.has_value());
        REQUIRE(*ex.p == 30ns);
    }
}

struct T06First {
    int a;
    int b;
    K2_SERIALIZABLE_FMT(T06First, a, b);
};
struct T06Second {
    K2_SERIALIZABLE_FMT(T06Second);
};

SCENARIO("Test 06: top level tuple") {
    skv::http::Binary buf;

    {
      std::tuple<T06First, T06Second> t06{T06First{.a=1,.b=5}, T06Second{}};
        skv::http::MPackWriter w;
        w.write(t06);
        REQUIRE(w.flush(buf));
    }
    {
      std::tuple<T06First, T06Second> t06Read{};
        skv::http::MPackReader reader(buf);
        REQUIRE(reader.read(t06Read));
        auto&&[first, second] = t06Read;
        REQUIRE(first.a == 1);
        REQUIRE(first.b == 5);
    }
}

