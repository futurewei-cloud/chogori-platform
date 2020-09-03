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

#include <k2/common/Log.h>
#include <k2/common/Common.h>

#include "FieldTypes.h"

namespace k2 {
namespace dto {

static constexpr char ESCAPE = '\0';
static constexpr char TERM = 0x01;
static constexpr char ESCAPED_NULL = 0xFF;

template <> FieldType TToFieldType<String>() { return FieldType::STRING; }
template <> FieldType TToFieldType<uint32_t>() { return FieldType::UINT32T; }
template <> FieldType TToFieldType<uint64_t>() { return FieldType::UINT64T; }
template <> FieldType TToFieldType<int32_t>() { return FieldType::INT32T; }
template <> FieldType TToFieldType<float>() { return FieldType::FLOAT; }

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
    String escapedString(String::initialized_later(), field.size() + foundNulls.size() + 3);
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

// Simple conversion to big-endian
template <> String FieldToKeyString<uint32_t>(const uint32_t& field)
{
    // type byte + 4 bytes + ESCAPE + TERM
    String s(String::initialized_later(), 7);
    s[0] = (char) FieldType::UINT32T;
    s[1] = (char)(field >> 24);
    s[2] = (char)(field >> 16);
    s[3] = (char)(field >> 8);
    s[4] = (char)(field);
    s[5] = ESCAPE;
    s[6] = TERM;

    return s;
}

// Simple conversion to big-endian
template <> String FieldToKeyString<uint64_t>(const uint64_t& field)
{
    // type byte + 8 bytes + ESCAPE + TERM
    String s(String::initialized_later(), 11);
    s[0] = (char) FieldType::UINT32T;
    s[1] = (char)(field >> 56);
    s[2] = (char)(field >> 48);
    s[3] = (char)(field >> 40);
    s[4] = (char)(field >> 32);
    s[5] = (char)(field >> 24);
    s[6] = (char)(field >> 16);
    s[7] = (char)(field >> 8);
    s[8] = (char)(field);
    s[9] = ESCAPE;
    s[10] = TERM;

    return s;
}

template <> String FieldToKeyString<int32_t>(const int32_t& field) {
    (void) field;
    throw new std::runtime_error("Key encoding for int32_t is not implemented yet");
}

template <> String FieldToKeyString<float>(const float& field) {
    (void) field;
    throw new std::runtime_error("Key encoding for float is not implemented yet");
}

String NullFirstToKeyString() {
    String s(String::initialized_later(), 3);
    s[0] = (char) FieldType::NULL_T;
    s[1] = ESCAPE;
    s[2] = TERM;
    return s;
}

String NullLastToKeyString() {
    String s(String::initialized_later(), 3);
    s[0] = (char) FieldType::NULL_LAST;
    s[1] = ESCAPE;
    s[2] = TERM;
    return s;
}

} // ns dto
} // ns k2
