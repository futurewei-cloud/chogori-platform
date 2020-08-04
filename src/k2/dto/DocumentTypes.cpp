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

#include <cstdint>
#include <cstring>

#include <k2/common/Log.h>
#include <k2/common/Common.h>

#include "DocumentTypes.h"

namespace k2 {
namespace dto {

template <> DocumentFieldType TToDocumentFieldType<String>() { return DocumentFieldType::STRING; }
template <> DocumentFieldType TToDocumentFieldType<uint32_t>() { return DocumentFieldType::UINT32T; }

template <> String DocumentFieldToKeyStringAscend<String>(const String& field) {
    // Sizes will not match if there are exta null bytes
    K2ASSERT(field.size() == strlen(field.c_str()), "String has null bytes");
    String typeByte("0");
    typeByte[0] = (char) DocumentFieldType::STRING;
    return typeByte+field;
}

// Simple conversion to big-endian
template <> String DocumentFieldToKeyStringAscend<uint32_t>(const uint32_t& field)
{
    // type byte + 4 bytes
    String s("12345");
    s[0] = (char) DocumentFieldType::UINT32T;
    s[1] = (char)(field >> 24);
    s[2] = (char)(field >> 16);
    s[3] = (char)(field >> 8);
    s[4] = (char)(field);

    return s;
}

String NullToKeyStringAscend() {
    // TODO double check this is two NULL bytes
    String s("\0");
    return s;
}

} // ns dto
} // ns k2
