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

#include <k2/dto/shared/Status.h>

#include "MySKVRecord.h"

namespace k2 {

class K2TxnHandle;

class K2HTTPClient {
public:
    std::future<Status> makeCollection(dto::CollectionMetadata&& metadata,  std::vector<String>&& rangeEnds=std::vector<String>());
    std::future<K2TxnHandle> beginTxn();
    static constexpr int64_t ANY_VERSION = -1;
    std::future<std::tuple<Status, dto::Schema>> getSchema(const String& collectionName, const String& schemaName, int64_t schemaVersion);
    std::future<Status> createSchema(const String& collectionName, dto::Schema schema);
};


class K2TxnHandle {
public:
    std::future<std::tuple<Status, SKVRecord>> read(SKVRecord& record);
    std::future<Status> write(SKVRecord& record, bool erase=false,
                              dto::ExistencePrecondition precondition=dto::ExistencePrecondition::None);
    std::future<Status> partialUpdate(SKVRecord& record, std::vector<k2::String>& fieldsName);
    // TODO query

    // Must be called exactly once by application code and after all ongoing read and write
    // operations are completed
    seastar::future<Status> end(bool shouldCommit);
};

} // namespace k2
