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
#pragma once
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <stdexcept>
#include <type_traits>

#include "AutoGenFormattingUtils.h"

// helper function for converting enum class into an integral type
// e.g. usage: auto integralColor = to_integral(MyEnum::Red);
// or  std::array<MyEnum, to_integral(MyEnum::Red)>;
template <typename T>
inline auto to_integral(T e) { return static_cast<std::underlying_type_t<T>>(e); }

// Intrusive formatting for classes
// Generates formatting and from/to json conversion methods for a given _K2_CTYPE_ARG, in a given namespace
// e.g. usage:
// namespace k2::dto {
// struct GetSchemaRequest() {
//    String schemaName;
//    K2_DEF_FMT(GetSchemaRequest, schemaName)
// };
// }
#define K2_DEF_FMT(_K2_CTYPE_ARG, ...)                                                            \
    template <typename OStream_T>                                                                 \
    friend OStream_T& operator<<(OStream_T& os, const _K2_CTYPE_ARG& o) {                         \
        if constexpr (std::is_same<OStream_T, std::ostream>::value) {                             \
            fmt::print(os,                                                                        \
                       FMT_STRING("{{" _K2_MKLIST(__VA_ARGS__) "}}") _K2_MKVARS(__VA_ARGS__));    \
        } else {                                                                                  \
            fmt::format_to(os.out(),                                                              \
                         FMT_COMPILE("{{" _K2_MKLIST(__VA_ARGS__) "}}") _K2_MKVARS(__VA_ARGS__)); \
        }                                                                                         \
        return os;                                                                                \
    }                                                                                             \
    friend void to_json(nlohmann::json& j, const _K2_CTYPE_ARG& o) {                              \
        (void)o;                                                                                  \
        j = nlohmann::json{_K2_TO_JSON(__VA_ARGS__)};                                             \
    }                                                                                             \
                                                                                                  \
    friend void from_json(const nlohmann::json& j, _K2_CTYPE_ARG& o) {                            \
        _K2_FROM_JSON(__VA_ARGS__)                                                                \
    }

// Generating enums with formatting. the _IC version is to be used when inside a class.
// e.g.
// namespace k2::dto {
// K2_DEF_ENUM(TxnStatus, Created, Aborted, Committed) )
// }
#define K2_DEF_ENUM_IC(_K2_ENUM_TYPE_NAME, ...)                                                     \
    enum class _K2_ENUM_TYPE_NAME {                                                                 \
        __VA_ARGS__                                                                                 \
    };                                                                                              \
    inline static const char* const _K2_ENUM_TYPE_NAME##Names[] = {                                 \
        _K2_TO_STRING_LIST(__VA_ARGS__)};                                                           \
    inline static _K2_ENUM_TYPE_NAME _K2_ENUM_TYPE_NAME##FromStr(const k2::String& str) {           \
        _K2_ENUM_IF_STMT(_K2_ENUM_TYPE_NAME, ##__VA_ARGS__);                                        \
        std::string s = fmt::format("unsupported value:{} in enum {}", str, #_K2_ENUM_TYPE_NAME);   \
        throw std::runtime_error(s.c_str());                                                        \
    }                                                                                               \
    inline static void to_json(nlohmann::json& j, const _K2_ENUM_TYPE_NAME& o) {                    \
        (void)o;                                                                                    \
        j["value"] = _K2_ENUM_TYPE_NAME##Names[to_integral(o)];                                     \
    }                                                                                               \
    inline static void from_json(const nlohmann::json& j, _K2_ENUM_TYPE_NAME& o) {                  \
        k2::String strname;                                                                         \
        j.at("value").get_to(strname);                                                              \
        o = _K2_ENUM_TYPE_NAME##FromStr(strname);                                                   \
    }                                                                                               \
    template <typename OStream_T>                                                                   \
    friend inline OStream_T& operator<<(OStream_T& os, const _K2_ENUM_TYPE_NAME& o) {               \
        if constexpr (std::is_same<OStream_T, std::ostream>::value) {                               \
            fmt::print(os, FMT_STRING("{}"), _K2_ENUM_TYPE_NAME##Names[to_integral(o)]);            \
        } else {                                                                                    \
            fmt::format_to(os.out(), FMT_COMPILE("{}"), _K2_ENUM_TYPE_NAME##Names[to_integral(o)]); \
        }                                                                                           \
        return os;                                                                                  \
    }

#define K2_DEF_ENUM(_K2_ENUM_TYPE_NAME, ...)                                                        \
    enum class _K2_ENUM_TYPE_NAME {                                                                 \
        __VA_ARGS__                                                                                 \
    };                                                                                              \
    inline static const char* const _K2_ENUM_TYPE_NAME##Names[] = {                                 \
        _K2_TO_STRING_LIST(__VA_ARGS__)};                                                           \
    inline static _K2_ENUM_TYPE_NAME _K2_ENUM_TYPE_NAME##FromStr(const k2::String& str) {           \
        _K2_ENUM_IF_STMT(_K2_ENUM_TYPE_NAME, ##__VA_ARGS__);                                        \
        std::string s = fmt::format("unsupported value:{} in enum {}", str, #_K2_ENUM_TYPE_NAME);   \
        throw std::runtime_error(s.c_str());                                                        \
    }                                                                                               \
    inline static void to_json(nlohmann::json& j, const _K2_ENUM_TYPE_NAME& o) {                    \
        (void)o;                                                                                    \
        j["value"] = _K2_ENUM_TYPE_NAME##Names[to_integral(o)];                                     \
    }                                                                                               \
    inline static void from_json(const nlohmann::json& j, _K2_ENUM_TYPE_NAME& o) {                  \
        k2::String strname;                                                                         \
        j.at("value").get_to(strname);                                                              \
        o = _K2_ENUM_TYPE_NAME##FromStr(strname);                                                   \
    }                                                                                               \
    template <typename OStream_T>                                                                   \
    inline OStream_T& operator<<(OStream_T& os, const _K2_ENUM_TYPE_NAME& o) {                      \
        if constexpr (std::is_same<OStream_T, std::ostream>::value) {                               \
            fmt::print(os, FMT_STRING("{}"), _K2_ENUM_TYPE_NAME##Names[to_integral(o)]);            \
        } else {                                                                                    \
            fmt::format_to(os.out(), FMT_COMPILE("{}"), _K2_ENUM_TYPE_NAME##Names[to_integral(o)]); \
        }                                                                                           \
        return os;                                                                                  \
    }
