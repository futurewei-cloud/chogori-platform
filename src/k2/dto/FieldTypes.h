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

#include <k2/common/Log.h>
#include <k2/common/Common.h>

namespace k2 {
namespace dto {

enum class FieldType : uint8_t {
    NULL_T = 0,
    STRING, // NULL characters in string is OK
    INT16T,
    INT32T,
    INT64T,
    FLOAT, // Not supported as key field for now
    DOUBLE,  // Not supported as key field for now
    BOOL,
    FIELD_TYPE,
    NOT_KNOWN = 254,
    NULL_LAST = 255
};

template <typename T>
FieldType TToFieldType();

// Converts a field type to a string suitable for being part of a key
template <typename T>
String FieldToKeyString(const T& field);

String NullFirstToKeyString();
String NullLastToKeyString();

} // ns dto
} // ns k2

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
