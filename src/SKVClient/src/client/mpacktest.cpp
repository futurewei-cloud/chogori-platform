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
#include <iostream>
#include <string>

#include <mpack/MPackSerialization.h>

std::string writeData(bool flag, uint32_t val) {
    char* data;
    size_t size;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &data, &size);

    // write the example on the msgpack homepage
    mpack_write_cstr(&writer, "compact");
    mpack_write_bool(&writer, flag);
    mpack_write_cstr(&writer, "val");
    mpack_write_u32(&writer, val);

    // finish writing
    if (mpack_writer_destroy(&writer) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "An error occurred encoding the data!");
        return std::string{};
    }

    // use the data
    std::string res(data, size);
    free(data);
    return res;
}
std::string readStr(mpack_node_t node) {
    size_t sz = mpack_node_strlen(node);
    if (mpack_node_error(node) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "invalid str size");
        return {};
    }
    const char* data = mpack_node_str(node);
    if (mpack_node_error(node) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "invalid str data");
        return {};
    }
    return std::string(data, sz);
}

void test1() {
    K2LOG_I(k2::log::mpack, "test1");
    auto s1 = writeData(true, 10);
    auto s2 = writeData(false, 20);
    auto buf = s1 + s2;
    mpack_tree_t tree;
    mpack_tree_init_data(&tree, buf.data(), buf.size());  // initialize a parser + parse a tree
    // read an entire node tree as a single object.
    mpack_tree_parse(&tree);
    auto node = mpack_tree_root(&tree);
    if (mpack_tree_error(&tree) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "error in tree parse");
        return;
    }

    std::cerr << readStr(node) << std::endl;
    mpack_tree_parse(&tree);
    node = mpack_tree_root(&tree);
    if (mpack_tree_error(&tree) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "error in tree parse");
        return;
    }

    bool b = mpack_node_bool(node);
    if (mpack_node_error(node) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "invalid bool");
        return;
    }
    std::cerr << b << std::endl;

    std::cerr << readStr(node) << std::endl;

    uint32_t ui = mpack_node_u32(node);
    if (mpack_node_error(node) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "invalid ui");
        return;
    }
    std::cerr << ui << std::endl;
    uint32_t ui2 = mpack_node_u32(node);
    if (mpack_node_error(node) != mpack_ok) {
        K2LOG_E(k2::log::mpack, "invalid ui");
        return;
    }
    K2LOG_I(k2::log::mpack, "ui2 {}", ui2);
    return;
}

void test2() {
    K2LOG_I(k2::log::mpack, "test2");
    k2::Binary buf1;
    {
        k2::MPackWriter w;
        w.write(10);
        w.write(k2::String("abcd"));
        std::optional<int> o1;
        std::optional<int> o2 = std::make_optional<int>(20);
        w.write(o1);
        w.write(o2);
        auto fr = w.flush(buf1);
        K2LOG_I(k2::log::mpack, "flushResult = {}", fr);
    }
    k2::MPackReader r(buf1);
    int x = 0;
    k2::String s;
    auto rr = r.read(x);
    auto rr2 = r.read(s);
    std::optional<int> o1, o2;
    auto rr3 = r.read(o1);
    auto rr4 = r.read(o2);
    K2LOG_I(k2::log::mpack, "readResult = {}, {}, {}, {}/ {}, {}, {}, {}", rr, rr2, rr3, rr4, x, s, o1.has_value(), o2.value());
}

struct Ex1 {
    K2_PAYLOAD_FIELDS();
    K2_DEF_FMT(Ex1);
};
struct Ex2 {
    int a;
    k2::String b;
    std::vector<k2::String> b2;
    std::vector<bool> b3;
    std::vector<int> b4;
    Ex1 c;
    K2_PAYLOAD_FIELDS(a, b, b2, b3, b4, c);
    K2_DEF_FMT(Ex2, a, b, b2, b3, b4, c);
};
struct Ex3 {
    std::vector<Ex2> a;
    std::list<Ex2> a1;
    std::deque<Ex2> a2;
    std::map<k2::String, Ex2> b;
    std::unordered_map<k2::String, Ex2> b1;
    std::set<k2::String> b2;
    std::unordered_set<k2::String> b3;
    std::tuple<int, Ex1, Ex2, k2::String, std::map<int, Ex2>> b4;
    uint8_t c;
    int8_t d;
    uint16_t e;
    int16_t f;
    uint32_t g;
    int32_t h;
    uint64_t i;
    int64_t j;
    std::decimal::decimal64 k;
    std::decimal::decimal128 l;
    k2::Binary m;
    K2_DEF_ENUM_IC(Action, A1, A2);
    Action n;
    K2_PAYLOAD_FIELDS(a, a1, a2, b, b1, b2, b3, b4, c, d, e, f, g, h, i, j, k, l, m, n);
    K2_DEF_FMT(Ex3, a, a1, a2, b, b1, b2, b3, b4, c, d, e, f, g, h, i, j, k, l, m, n);
};

void test3() {
    K2LOG_I(k2::log::mpack, "test3");
    k2::Binary buf1;
    {
        Ex3 ex3{};
        k2::MPackWriter w;
        w.write(ex3);
        auto fr = w.flush(buf1);
        K2LOG_I(k2::log::mpack, "flushResult = {}", fr);
    }

    Ex2 ex3{};
    k2::MPackReader reader(buf1);
    reader.read(ex3);
    K2LOG_I(k2::log::mpack, "read {}", ex3);
}

int main() {
    test1();
    test2();
    test3();
    return 0;
}
