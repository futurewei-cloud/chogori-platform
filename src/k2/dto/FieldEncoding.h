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

#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/dto/shared/FieldTypes.h>

namespace k2 {
namespace dto {

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

} // ns dto
} // ns k2

