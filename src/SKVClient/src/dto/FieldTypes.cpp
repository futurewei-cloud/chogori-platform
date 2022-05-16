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

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "FieldTypes.h"

namespace skv::http {
namespace dto {

static constexpr char ESCAPE = '\0';
static constexpr char TERM = 0x01;
static constexpr char ESCAPED_NULL = 0xFF;
// + > -, and not using the above constants just for potentially easier decoding
static constexpr char SIGN_POS = 0x03;
static constexpr char SIGN_NEG = 0x02;

template <> FieldType TToFieldType<String>() { return FieldType::STRING; }
template <> FieldType TToFieldType<int16_t>() { return FieldType::INT16T; }
template <> FieldType TToFieldType<int32_t>() { return FieldType::INT32T; }
template <> FieldType TToFieldType<int64_t>() { return FieldType::INT64T; }
template <> FieldType TToFieldType<float>() { return FieldType::FLOAT; }
template <> FieldType TToFieldType<double>() { return FieldType::DOUBLE; }
template <> FieldType TToFieldType<bool>() { return FieldType::BOOL; }
template <> FieldType TToFieldType<std::decimal::decimal64>() { return FieldType::DECIMAL64; }
template <> FieldType TToFieldType<std::decimal::decimal128>() { return FieldType::DECIMAL128; }
template <> FieldType TToFieldType<FieldType>() { return FieldType::FIELD_TYPE; }

// All conversion assume ascending ordering

template <> String FieldToKeyString<String>(const String& field) {
    size_t pos = 0;
    std::vector<size_t> foundNulls;

    while (pos != String::npos && pos < field.size()) {
        pos = field.find(ESCAPE, pos);
        if (pos != String::npos) {
            foundNulls.push_back(pos);
            ++pos;
        }
    }

    // Size is original +1 type byte +1 byte per null and +2 terminator bytes
    String escapedString;
    escapedString.resize(field.size() + foundNulls.size() + 3);
    size_t originalCursor = 0;
    size_t escapedCursor = 1;
    for (size_t nullPos : foundNulls) {
        std::copy(field.begin() + originalCursor, field.begin() + nullPos + 1,
                  escapedString.begin() + escapedCursor);

        size_t count = nullPos - originalCursor + 1;
        originalCursor += count;
        escapedCursor += count;
        escapedString[escapedCursor] = ESCAPED_NULL;
        ++escapedCursor;
    }

    if (originalCursor < field.size()) {
        std::copy(field.begin() + originalCursor, field.end(),
                  escapedString.begin() + escapedCursor);
    }

    escapedString[0] = (char) FieldType::STRING;
    escapedString[escapedString.size() - 2] = ESCAPE;
    escapedString[escapedString.size() - 1] = TERM;

    return escapedString;
}

// Strategy for intxx_t conversions:
// Add a sign byte
// If -, convert to +
// Convert to big endian
// If originally -, subtract each byte from 255
// 0 is encoded as +0

template <> String FieldToKeyString<int16_t>(const int16_t& field)
{
    // type byte + sign byte + 2 bytes + ESCAPE + TERM
    String s;
    s.resize(6);
    s[0] = (char) FieldType::INT16T;

    if (field >= 0) {
        s[1] = SIGN_POS;
        s[2] = (char)(field >> 8);
        s[3] = (char)(field);
    } else {
        s[1] = SIGN_NEG;
        int pos_field = -field;
        s[2] = 255-((char)(pos_field >> 8));
        s[3] = 255-((char)(pos_field));
    }

    s[4] = ESCAPE;
    s[5] = TERM;

    return s;
}

template <> String FieldToKeyString<int64_t>(const int64_t& field)
{
    // type byte + sign byte + 8 bytes + ESCAPE + TERM
    String s;
    s.resize(12);
    s[0] = (char) FieldType::INT64T;

    if (field >= 0) {
        s[1] = SIGN_POS;
        s[2] = (char)(field >> 56);
        s[3] = (char)(field >> 48);
        s[4] = (char)(field >> 40);
        s[5] = (char)(field >> 32);
        s[6] = (char)(field >> 24);
        s[7] = (char)(field >> 16);
        s[8] = (char)(field >> 8);
        s[9] = (char)(field);
    } else {
        s[1] = SIGN_NEG;
        int64_t pos_field = -field;
        s[2] = 255-((char)(pos_field >> 56));
        s[3] = 255-((char)(pos_field >> 48));
        s[4] = 255-((char)(pos_field >> 40));
        s[5] = 255-((char)(pos_field >> 32));
        s[6] = 255-((char)(pos_field >> 24));
        s[7] = 255-((char)(pos_field >> 16));
        s[8] = 255-((char)(pos_field >> 8));
        s[9] = 255-((char)(pos_field));
    }

    s[10] = ESCAPE;
    s[11] = TERM;

    return s;
}

template <> String FieldToKeyString<int32_t>(const int32_t& field) {
    // type byte + sign byte + 4 bytes + ESCAPE + TERM
    String s;
    s.resize(8);
    s[0] = (char) FieldType::INT32T;

    if (field >= 0) {
        s[1] = SIGN_POS;
        s[2] = (char)(field >> 24);
        s[3] = (char)(field >> 16);
        s[4] = (char)(field >> 8);
        s[5] = (char)(field);
    } else {
        s[1] = SIGN_NEG;
        int64_t pos_field = -field;
        s[2] = 255-((char)(pos_field >> 24));
        s[3] = 255-((char)(pos_field >> 16));
        s[4] = 255-((char)(pos_field >> 8));
        s[5] = 255-((char)(pos_field));
    }

    s[6] = ESCAPE;
    s[7] = TERM;

    return s;
}

template <> String FieldToKeyString<bool>(const bool& field) {
    String s;
    s.resize(4);
    s[0] = (char) FieldType::BOOL;
    s[1] = field ? 1 : 0;
    s[2] = ESCAPE;
    s[3] = TERM;
    return s;
}

String NullFirstToKeyString() {
    String s;
    s.resize(3);
    s[0] = (char) FieldType::NULL_T;
    s[1] = ESCAPE;
    s[2] = TERM;
    return s;
}

String NullLastToKeyString() {
    String s;
    s.resize(3);
    s[0] = (char) FieldType::NULL_LAST;
    s[1] = ESCAPE;
    s[2] = TERM;
    return s;
}

} // ns dto
} // ns k2
