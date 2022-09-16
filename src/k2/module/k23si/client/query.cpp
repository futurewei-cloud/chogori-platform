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

#include "query.h"
#include "k23si_client.h"


namespace k2 {

void Query::setFilterExpression(dto::expression::Expression&& root) {
    request.filterExpression = std::move(root);
}

void Query::setReverseDirection(bool reverseDirection) {
    request.reverseDirection = reverseDirection;
}

void Query::setIncludeVersionMismatch(bool includeVersionMismatch) {
    request.includeVersionMismatch = includeVersionMismatch;
}

void Query::setLimit(int32_t limit) {
    request.recordLimit = limit;
}

void Query::addProjection(const String& fieldName) {
    request.projection.push_back(fieldName);
    checkKeysProjected();
}

void Query::addProjection(const std::vector<String>& fieldNames) {
    for (const String& name : fieldNames) {
        request.projection.push_back(name);
    }
    checkKeysProjected();
}

void Query::checkKeysProjected() {
    keysProjected = false;
    for (uint32_t idx : schema->partitionKeyFields) {
        String& name = schema->fields[idx].name;
        bool found = false;
        for (const String& projected : request.projection) {
            if (projected == name) {
                found = true;
            }
        }

        if (!found) {
            return;
        }
    }
    for (uint32_t idx : schema->rangeKeyFields) {
        String& name = schema->fields[idx].name;
        bool found = false;
        for (const String& projected : request.projection) {
            if (projected == name) {
                found = true;
            }
        }

        if (!found) {
            return;
        }
    }

    keysProjected = true;
}

bool Query::isDone() {
    return done;
}

void Query::resetPaginationToken(dto::Key paginationKey, bool exclusiveKey) {
    // If query is not progress, then the user could have never gotten a valid pagination token to use here
    if (inprogress) {
        done = false;
        request.key = std::move(paginationKey);
        request.exclusiveKey = exclusiveKey;
    }
}

std::tuple<dto::Key, bool> Query::getPaginationToken() {
    bool exclusive = request.exclusiveKey;
    return std::make_tuple<dto::Key, bool>(dto::Key(request.key), std::move(exclusive));
}

seastar::future<QueryResult> QueryResult::makeQueryResult(K23SIClient* client, const Query& query, Status status, dto::K23SIQueryResponse&& response) {
    std::vector<seastar::future<>> futures;
    QueryResult* result = new QueryResult(std::move(status));

    for (dto::SKVRecord::Storage& storage : response.results) {
        futures.push_back(client->getSchema(query.request.collectionName,
                    query.schema->name, storage.schemaVersion)
        .then([&query, result, s=std::move(storage)] (GetSchemaResult&& get_response) mutable {
            if (!get_response.status.is2xxOK()) {
                result->status = std::move(get_response.status);
                return seastar::make_ready_future<>();
            }

            dto::SKVRecord record(query.request.collectionName, get_response.schema, std::move(s), query.keysProjected);
            result->records.emplace_back(std::move(record));
            return seastar::make_ready_future<>();
        }));
    }

    return seastar::when_all_succeed(futures.begin(), futures.end())
    .then([result] () {
        return seastar::make_ready_future<QueryResult>(std::move(*result));
    })
    .finally([result] () {
        delete result;
    });
}

} // namespace k2
