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

#pragma once

#include <algorithm>
#include <decimal/decimal>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <set>

#include "Chrono.h"

namespace std {
inline ostream& operator<<(ostream& os, const decimal::decimal64& d) {
    decimal::decimal64::__decfloat64 data = const_cast<decimal::decimal64&>(d).__getval();
    return os << (double)data;
}
inline ostream& operator<<(ostream& os, const decimal::decimal128& d) {
    decimal::decimal128::__decfloat128 data = const_cast<decimal::decimal128&>(d).__getval();
    return os << (double)data;
}
}

#define DISABLE_COPY(className)           \
    className(const className&) = delete; \
    className& operator=(const className&) = delete;

#define DISABLE_MOVE(className)      \
    className(className&&) = delete; \
    className& operator=(className&&) = delete;

#define DISABLE_COPY_MOVE(className) \
    DISABLE_COPY(className)          \
    DISABLE_MOVE(className)

#define DEFAULT_COPY(className)            \
    className(const className&) = default; \
    className& operator=(const className&) = default;

#define DEFAULT_MOVE(className)       \
    className(className&&) = default; \
    className& operator=(className&&) = default;

#define DEFAULT_COPY_MOVE(className) \
    DEFAULT_COPY(className)          \
    DEFAULT_MOVE(className)

#define DEFAULT_COPY_MOVE_INIT(className) \
    className() {}                        \
    DEFAULT_COPY(className)               \
    DEFAULT_MOVE(className)

namespace k2 {

//
//  K2 general string type
//
typedef seastar::sstring String;

//
//  Binary represents owned (not referenced) binary data
//
typedef seastar::temporary_buffer<char> Binary;

//
// The type for a function which can allocate Binary
//
typedef std::function<Binary()> BinaryAllocatorFunctor;

// from https://stackoverflow.com/questions/180947/base64-decode-snippet-in-c/34571089#34571089
inline const char* B64chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

inline const int B64index[256] =
    {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 63, 62, 62, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 0, 0, 0, 0, 0, 0,
        0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 0, 0, 0, 0, 63,
        0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51};

inline const std::string b64encode(const void* data, const size_t len) {
    std::string result((len + 2) / 3 * 4, '=');
    char* p = (char*)data;
    char* str = &result[0];
    size_t j = 0, pad = len % 3;
    const size_t last = len - pad;

    for (size_t i = 0; i < last; i += 3) {
        int n = int(p[i]) << 16 | int(p[i + 1]) << 8 | p[i + 2];
        str[j++] = B64chars[n >> 18];
        str[j++] = B64chars[n >> 12 & 0x3F];
        str[j++] = B64chars[n >> 6 & 0x3F];
        str[j++] = B64chars[n & 0x3F];
    }
    if (pad) {
        int n = --pad ? int(p[last]) << 8 | p[last + 1] : p[last];
        str[j++] = B64chars[pad ? n >> 10 & 0x3F : n >> 2];
        str[j++] = B64chars[pad ? n >> 4 & 0x03F : n << 4 & 0x3F];
        str[j++] = pad ? B64chars[n << 2 & 0x3F] : '=';
    }
    return result;
}

inline std::string b64decode(const void* data, const size_t len) {
    if (len == 0) return "";

    unsigned char* p = (unsigned char*)data;
    size_t j = 0,
           pad1 = len % 4 || p[len - 1] == '=',
           pad2 = pad1 && (len % 4 > 2 || p[len - 2] != '=');
    const size_t last = (len - pad1) / 4 << 2;
    std::string result(last / 4 * 3 + pad1 + pad2, '\0');
    unsigned char* str = (unsigned char*)&result[0];

    for (size_t i = 0; i < last; i += 4) {
        int n = B64index[p[i]] << 18 | B64index[p[i + 1]] << 12 | B64index[p[i + 2]] << 6 | B64index[p[i + 3]];
        str[j++] = n >> 16;
        str[j++] = n >> 8 & 0xFF;
        str[j++] = n & 0xFF;
    }
    if (pad1) {
        int n = B64index[p[last]] << 18 | B64index[p[last + 1]] << 12;
        str[j++] = n >> 16;
        if (pad2) {
            n |= B64index[p[last + 2]] << 6;
            str[j++] = n >> 8 & 0xFF;
        }
    }
    return result;
}

}  //  namespace k2

template <> // json (de)serialization support
struct nlohmann::adl_serializer<k2::String> {
    // since we transport raw/binary with strings, we just b64 encode all strings in json
    static void to_json(nlohmann::json& j, const k2::String& str) {
        j = k2::b64encode(str.data(), str.size());
    }

    static void from_json(const nlohmann::json& j, k2::String& str) {
        auto result = j.get<std::string>();
        str = k2::b64decode(result.data(), result.size());
    }
};

template <> // fmt support
struct fmt::formatter<k2::String> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(k2::String const& str, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "{}", str.data());
    }
};

template <> // fmt support
struct fmt::formatter<std::set<k2::String>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(std::set<k2::String> const& str, FormatContext& ctx) {
        fmt::format_to(ctx.out(), "{{");
        const auto it = str.begin();
        size_t processed = 0;
        while(it != str.end()) {
            if (processed == str.size() - 1) {
                fmt::format_to(ctx.out(), "{}", *it);
            }
            else {
                fmt::format_to(ctx.out(), "{}, ", *it);
            }
        }
        return fmt::format_to(ctx.out(), "}}");
    }
};
