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

enum class DocumentFieldType : uint8_t {
    NULL_T = 0,
    STRING = 1,
    UINT32T = 2,
    MAX_TYPE = 127
    // Last bit is used to determine ascending/descending
};

template <typename T>
DocumentFieldType TToDocumentFieldType() {
    K2ASSERT(false, "Unsupported type for document");
}

template <> DocumentFieldType TToDocumentFieldType<String>() { return DocumentFieldType::STRING; }
template <> DocumentFieldType TToDocumentFieldType<uint32_t>() { return DocumentFieldType::UINT32T; }

// Converts a document field type to a string suitable for being part of a key
template <typename T>
String DocumentFieldToKeyStringAscend(const T& field) {
    K2ASSERT(false, "Unsupported type for document");
}

template <> String DocumentFieldToKeyStringAscend<String>(const String& field) {
    // Sizes will not match if there are exta null bytes
    K2ASSERT(field.size() == strlen(field.c_str()));
    String typeByte("0");
    typeByte[0] = DocumentFieldType::STRING;
    return typeByte+field;
}

// Simple conversion to big-endian
template <> String DocumentFieldToKeyStringAscend<uint32_t>(const uint32_t& field)
{
    // type byte + 4 bytes
    String s("12345");
    s[0] = DocumentFieldType::UINT32T;
    s[1] = (uint8_t)(field >> 24);
    s[2] = (uint8_t)(field >> 16);
    s[3] = (uint8_t)(field >> 8);
    s[4] = (uint8_t)(field);

    return s;
}

String NullToKeyStringAscend() {
    String s("n");
    s[0] = '\0';
    return s;
}

} // ns dto
} // ns k2
