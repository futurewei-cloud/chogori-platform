/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

#undef K2_PLATFORM_COMPILE
#include "SKVStorage.h"
#include <k2/dto/shared/SKVRecord.h>

namespace k2 {

namespace dto {

template <typename T>
void SKVRecord::serializeNext(T field) {}

template <>
void SKVRecord::serializeNext(int32_t field) {
    storage.data.push_back(field);
}

template <typename T>
std::optional<T> SKVRecord::deserializeField(uint32_t fieldIndex) {return std::optional<T>();}

template <>
std::optional<int32_t> SKVRecord::deserializeField(uint32_t fieldIndex) {return storage.data[fieldIndex];}

SKVRecord::SKVRecord(const String& collection, std::shared_ptr<Schema> s) :
            schema(s), collectionName(collection), keyValuesAvailable(true), keyStringsConstructed(true) {
    partitionKeys.resize(schema->partitionKeyFields.size());
    rangeKeys.resize(schema->rangeKeyFields.size());
}

} // ns dto
} // ns k2
