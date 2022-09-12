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

#include <skvhttp/common/Common.h>

namespace skv::http {
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
    DECIMAL100, // Provides 34 decimal digits of precision
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

    if constexpr (std::is_same_v<T, Decimal64>)  { // handle NaN decimal
        if (std::isnan(Decimal64_to_float(field))) {
            return true;
        }
    }

    if constexpr (std::is_same_v<T, Decimal100> )  { // handle NaN decimal
        if (std::isnan(Decimal100_to_float(field))) {
            return true;
        }
    }

    return false;
}

template<typename T, typename FieldT>
struct AppliedFieldRef {
    using ValueT = typename std::remove_reference_t<T>;
    const FieldT& field;
    AppliedFieldRef(const FieldT& f): field(f){}
};

template <class T>
using applied_type_t = typename std::remove_reference_t<T>::ValueT;

template <typename FieldT, typename Func, typename ...Args>
auto applyTyped(const FieldT& field, Func&& applier, Args&&... args) {
    switch (field.type) {
        case FieldType::STRING: {
            return applier(AppliedFieldRef<String, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::INT16T: {
            return applier(AppliedFieldRef<int16_t, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::INT32T: {
            return applier(AppliedFieldRef<int32_t, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::INT64T: {
            return applier(AppliedFieldRef<int64_t, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::FLOAT: {
            return applier(AppliedFieldRef<float, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::DOUBLE: {
            return applier(AppliedFieldRef<double, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::BOOL: {
            return applier(AppliedFieldRef<bool, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::DECIMAL64: {
            return applier(AppliedFieldRef<Decimal64, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::DECIMAL100: {
            return applier(AppliedFieldRef<Decimal100, FieldT>(field), std::forward<Args>(args)...);
        }
        case FieldType::FIELD_TYPE: {
            return applier(AppliedFieldRef<FieldType, FieldT>(field), std::forward<Args>(args)...);
        }
        default:
            throw TypeMismatchException("unknown type");
    }
}

} // ns dto
} // ns k2

namespace std {
    inline std::ostream& operator<<(std::ostream& os, const skv::http::dto::FieldType& ftype) {
        switch(ftype) {
        case skv::http::dto::FieldType::NULL_T:
            return os << "NULL";
        case skv::http::dto::FieldType::STRING:
            return os << "STRING";
        case skv::http::dto::FieldType::INT16T:
            return os << "INT16T";
        case skv::http::dto::FieldType::INT32T:
            return os << "INT32T";
        case skv::http::dto::FieldType::INT64T:
            return os << "INT64T";
        case skv::http::dto::FieldType::FLOAT:
            return os << "FLOAT";
        case skv::http::dto::FieldType::DOUBLE:
            return os << "DOUBLE";
        case skv::http::dto::FieldType::BOOL:
            return os << "BOOL";
        case skv::http::dto::FieldType::DECIMAL64:
            return os << "DECIMAL64";
        case skv::http::dto::FieldType::DECIMAL100:
            return os << "DECIMAL100";
        case skv::http::dto::FieldType::FIELD_TYPE:
            return os << "FIELD_TYPE";
        case skv::http::dto::FieldType::NOT_KNOWN:
            return os << "NOT_KNOWN";
        case skv::http::dto::FieldType::NULL_LAST:
            return os << "NULL_LAST";
        default:
            return os << "UNKNOWN_FIELD_TYPE";
        };
    }
}
