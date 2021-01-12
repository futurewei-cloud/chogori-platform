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

#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include <k2/dto/K23SI.h>

namespace k2 {

// Represents a new or in-progress query (aka read scan with predicate and projection)
class Query {
public:
    void setFilterExpression(dto::expression::Expression&& root);
    void setReverseDirection(bool reverseDirection);
    void setIncludeVersionMismatch(bool includeVersionMismatch);
    void setLimit(int32_t limit);

    void addProjection(const String& fieldName);
    void addProjection(const std::vector<String>& fieldNames);

    bool isDone(); // If false, more results may be available

    // The user must specify the inclusive start and exclusive end keys for the range scan, but the client
    // still needs to encode these keys so we use SKVRecords. The SKVRecords will be created with an
    // appropriate schema by the client createQuery function. The user is then expected to serialize the
    // key fields into the SKVRecords, similar to a single key read request.
    //
    // They must be a fully specified prefix of the key fields. For example, if the key fields are defined
    // as {ID, NAME, TIMESTAMP} then {ID = 1, TIMESTAMP = 10} is not a valid start or end scanRecord, but
    // {ID = 1, NAME = J} is valid.
    dto::SKVRecord startScanRecord;
    dto::SKVRecord endScanRecord;

private:
    void checkKeysProjected();

    std::shared_ptr<dto::Schema> schema = nullptr;
    bool done = false;
    bool inprogress = false; // Used to prevent user from changing predicates after query has started
    bool keysProjected = true;
    dto::Key continuationToken;
    dto::K23SIQueryRequest request;

    friend class K2TxnHandle;
    friend class K23SIClient;
    friend class QueryResult;
};

class K23SIClient;

class QueryResult {
public:
    QueryResult(Status s) : status(std::move(s)) {}
    static seastar::future<QueryResult> makeQueryResult(K23SIClient* client, const Query& query, Status status, dto::K23SIQueryResponse&& response);

    Status status;
    std::vector<dto::SKVRecord> records;
};

} // namespace k2
