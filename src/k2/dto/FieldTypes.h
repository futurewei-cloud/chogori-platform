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

#include <cstdint>
#include <cstring>
#include <decimal/decimal>

#include <k2/logging/Log.h>
#include <k2/common/Common.h>

namespace k2 {
namespace dto {

struct SKVKeyEncodingException : public std::exception {
    String what_str;
    SKVKeyEncodingException(String s) : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override{ return what_str.c_str();}
};

struct FieldNotSupportedAsKeyException: public std::exception {
    String what_str;
    FieldNotSupportedAsKeyException(String s) : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

// Thrown when a field type doesn't match during lookup
struct TypeMismatchException : public std::exception {
    String what_str;
    TypeMismatchException(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

// This enum is used for:
// 1. User-specified field types in SKV schemas
// 2. Encoding of key fields
// 3. User-specified query expressions
// Not all enum values are used for all of the above uses. For example, NULL_T is used for
// key encoding but not as a schema field type.
enum class FieldType : uint8_t {
    NULL_T = 0,
    STRING, // NULL characters in string is OK
    INT16T,
    INT32T,
    INT64T,
    FLOAT, // Not supported as key field for now
    DOUBLE,  // Not supported as key field for now
    BOOL,
    DECIMAL64, // Provides 16 decimal digits of precision
    DECIMAL128, // Provides 34 decimal digits of precision
    FIELD_TYPE, // The value refers to one of these types. Used in query filters.
    NOT_KNOWN = 254,
    NULL_LAST = 255
};

template <typename T>
FieldType TToFieldType();

// Converts a field type to a string suitable for being part of a key
template<typename T>
String FieldToKeyString(const T&);

// these types are supported as key fields
template<> String FieldToKeyString<int16_t>(const int16_t&);
template<> String FieldToKeyString<int32_t>(const int32_t&);
template<> String FieldToKeyString<int64_t>(const int64_t&);
template<> String FieldToKeyString<String>(const String&);
template<> String FieldToKeyString<bool>(const bool&);

// all other types are not supported as key fields
template <typename T>
String FieldToKeyString(const T&) {
    throw FieldNotSupportedAsKeyException(fmt::format("Key encoding for {} not implemented yet", TToFieldType<T>()));
}

String NullFirstToKeyString();
String NullLastToKeyString();

// To prevent NaN in fields
template <typename T>
bool isNan(const T& field){
    if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double> )  { // handle NaN float and double
        if (std::isnan(field)) {
            return true;
        }
    }

    if constexpr (std::is_same_v<T, std::decimal::decimal64>)  { // handle NaN decimal
        if (std::isnan(std::decimal::decimal64_to_float(field))) {
            return true;
        }
    }

    if constexpr (std::is_same_v<T, std::decimal::decimal128> )  { // handle NaN decimal
        if (std::isnan(std::decimal::decimal128_to_float(field))) {
            return true;
        }
    }

    return false;
}

} // ns dto
} // ns k2

#define K2_DTO_CAST_APPLY_FIELD_VALUE(func, a, ...)                 \
    do {                                                            \
        switch ((a).type) {                                         \
            case k2::dto::FieldType::STRING: {                      \
                func<k2::String>((a), __VA_ARGS__);                 \
            } break;                                                \
            case k2::dto::FieldType::INT16T: {                      \
                func<int16_t>((a), __VA_ARGS__);                    \
            } break;                                                \
            case k2::dto::FieldType::INT32T: {                      \
                func<int32_t>((a), __VA_ARGS__);                    \
            } break;                                                \
            case k2::dto::FieldType::INT64T: {                      \
                func<int64_t>((a), __VA_ARGS__);                    \
            } break;                                                \
            case k2::dto::FieldType::FLOAT: {                       \
                func<float>((a), __VA_ARGS__);                      \
            } break;                                                \
            case k2::dto::FieldType::DOUBLE: {                      \
                func<double>((a), __VA_ARGS__);                     \
            } break;                                                \
            case k2::dto::FieldType::BOOL: {                        \
                func<bool>((a), __VA_ARGS__);                       \
            } break;                                                \
            case k2::dto::FieldType::DECIMAL64: {                   \
                func<std::decimal::decimal64>((a), __VA_ARGS__);    \
            } break;                                                \
            case k2::dto::FieldType::DECIMAL128: {                  \
                func<std::decimal::decimal128>((a), __VA_ARGS__);   \
            } break;                                                \
            case k2::dto::FieldType::FIELD_TYPE: {                  \
                func<k2::dto::FieldType>((a), __VA_ARGS__);         \
            } break;                                                \
            default:                                                \
                auto msg = fmt::format(                             \
                    "cannot apply field of type {}", (a).type);     \
                throw k2::dto::TypeMismatchException(msg);          \
        }                                                           \
    } while (0)

namespace std {
    inline std::ostream& operator<<(std::ostream& os, const k2::dto::FieldType& ftype) {
        switch(ftype) {
        case k2::dto::FieldType::NULL_T:
            return os << "NULL";
        case k2::dto::FieldType::STRING:
            return os << "STRING";
        case k2::dto::FieldType::INT16T:
            return os << "INT16T";
        case k2::dto::FieldType::INT32T:
            return os << "INT32T";
        case k2::dto::FieldType::INT64T:
            return os << "INT64T";
        case k2::dto::FieldType::FLOAT:
            return os << "FLOAT";
        case k2::dto::FieldType::DOUBLE:
            return os << "DOUBLE";
        case k2::dto::FieldType::BOOL:
            return os << "BOOL";
        case k2::dto::FieldType::DECIMAL64:
            return os << "DECIMAL64";
        case k2::dto::FieldType::DECIMAL128:
            return os << "DECIMAL128";
        case k2::dto::FieldType::FIELD_TYPE:
            return os << "FIELD_TYPE";
        case k2::dto::FieldType::NOT_KNOWN:
            return os << "NOT_KNOWN";
        case k2::dto::FieldType::NULL_LAST:
            return os << "NULL_LAST";
        default:
            return os << "UNKNOWN_FIELD_TYPE";
        };
    }
}
