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

#include "SKVClient.h"

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

using namespace skv::http;

boost::future<Response<>> Client::createCollection(dto::CollectionMetadata metadata, std::vector<String> rangeEnds) {
    dto::CollectionCreateRequest request{
        .metadata = std::move(metadata),
        .rangeEnds = std::move(rangeEnds)
    };

    return _HTTPClient.POST<dto::CollectionCreateRequest>("/api/CreateCollection", std::move(request));
}

boost::future<Response<>> Client::createSchema(const String& collectionName, const dto::Schema& schema) {
    dto::CreateSchemaRequest request{
        .collectionName = collectionName,
        .schema = schema
    };

    return _HTTPClient.POST<dto::CreateSchemaRequest>("/api/CreateSchema", std::move(request));
}

boost::future<Response<std::shared_ptr<dto::Schema>>> Client::getSchema(const String& collectionName, const String& schemaName, int64_t schemaVersion) {
    SchemaCacheT::const_iterator collectionIt = _schemaCache.find(collectionName);
    if (collectionIt != _schemaCache.end()) {
        auto schemaNameIt = collectionIt->second.find(schemaName);
        if (schemaNameIt != collectionIt->second.end()) {
            if (schemaVersion == dto::ANY_SCHEMA_VERSION && schemaNameIt->second.size()) {
                std::shared_ptr<dto::Schema> schema = schemaNameIt->second.begin()->second;
                return MakeResponse<std::shared_ptr<dto::Schema>>(Statuses::S200_OK("Got schema from cache"), std::move(schema));
            }

            auto schemaVersionIt = schemaNameIt->second.find(schemaVersion);
            if (schemaVersionIt != schemaNameIt->second.end()) {
                std::shared_ptr<dto::Schema> schema = schemaVersionIt->second;
                return MakeResponse<std::shared_ptr<dto::Schema>>(Statuses::S200_OK("Got schema from cache"), std::move(schema));
            }
        }
    }

    dto::GetSchemaRequest request{
        .collectionName = collectionName,
        .schemaName = schemaName,
        .schemaVersion = schemaVersion
    };

    return _HTTPClient.POST<dto::GetSchemaRequest, dto::GetSchemaResponse>("/api/GetSchema", std::move(request)).then([this, collectionName, schemaName, schemaVersion](auto&& futResp) {
        auto&& [status, resp] = futResp.get();
        if (status.is2xxOK()) {
            auto schema = std::make_shared<dto::Schema>(resp.schema);
            _schemaCache[collectionName][schemaName][schemaVersion] = schema;
            return Response<std::shared_ptr<dto::Schema>>(std::move(status), schema);
        }
        return Response<std::shared_ptr<dto::Schema>>(std::move(status), nullptr);
    });
}

boost::future<Response<TxnHandle>> Client::beginTxn(dto::TxnOptions options) {
    dto::TxnBeginRequest request{.options = std::move(options)};

    return _HTTPClient.POST<dto::TxnBeginRequest, dto::TxnBeginResponse>("/api/TxnBegin", std::move(request)).then([this](auto&& futResp) {
        auto&& [status, resp] = futResp.get();
        TxnHandle txn(this, resp.timestamp);
        K2LOG_D(log::shclient, "created txn: {}", txn);
        return Response<TxnHandle>(std::move(status), std::move(txn));
    });
}

boost::future<Response<>> TxnHandle::write(dto::SKVRecord& record, bool erase, dto::ExistencePrecondition precondition) {
    dto::WriteRequest request{
        .timestamp = _id,
        .collectionName = record.collectionName,
        .schemaName = record.schema->name,
        .isDelete = erase,
        .precondition = precondition,
        .value = record.storage.share(),
        .fieldsForPartialUpdate = std::vector<uint32_t>{},
    };

    return _client->_HTTPClient.POST<dto::WriteRequest>("/api/Write", std::move(request));
}

boost::future<Response<>> TxnHandle::partialUpdate(dto::SKVRecord& record, std::vector<uint32_t> fieldNamesForUpdate) {
    dto::WriteRequest request{
        .timestamp = _id,
        .collectionName = record.collectionName,
        .schemaName = record.schema->name,
        .isDelete = false,
        .precondition = dto::ExistencePrecondition::None,
        .value = record.storage.share(),
        .fieldsForPartialUpdate = fieldNamesForUpdate,
    };

    return _client->_HTTPClient.POST<dto::WriteRequest>("/api/Write", std::move(request));
}

boost::future<Response<dto::SKVRecord>> TxnHandle::read(dto::SKVRecord record) {
    dto::ReadRequest request{
        .timestamp = _id,
        .collectionName = record.collectionName,
        .schemaName = record.schema->name,
        .key = std::move(record.storage)
    };

    return _client->_HTTPClient.POST<dto::ReadRequest, dto::ReadResponse>("/api/Read", std::move(request))
      .then([this](auto&& futResp) {
          auto&& [status, resp] = futResp.get();
          if (!status.is2xxOK()) {
              return MakeResponse<dto::SKVRecord>(std::move(status), dto::SKVRecord{});
          }
          return _client->getSchema(resp.collectionName, resp.schemaName, resp.record.schemaVersion)
            .then([status, collName = resp.collectionName, storage = std::move(resp.record)](auto&& schemaFut) mutable {
              auto&& [schemaStatus, schemaResp] = schemaFut.get();
              if (!schemaStatus.is2xxOK()) {
                  return Response<dto::SKVRecord>(std::move(schemaStatus), dto::SKVRecord{});
              }

              std::shared_ptr<dto::Schema> schema = schemaResp;
              dto::SKVRecord record(collName, schema, std::move(storage), true);
              return Response<dto::SKVRecord>(std::move(status), std::move(record));
            });
      })
      .unwrap();
}

boost::future<Response<dto::QueryRequest>> TxnHandle::createQuery(dto::SKVRecord& startKey, dto::SKVRecord& endKey, dto::expression::Expression&& filter,
                                                                  std::vector<String>&& projection, int32_t recordLimit, bool reverseDirection, bool includeVersionMismatch) {
    if (startKey.collectionName != endKey.collectionName || startKey.schema->name != endKey.schema->name) {
        return MakeResponse(Statuses::S400_Bad_Request("Start and end keys must have same collection and schema"), dto::QueryRequest{});
    }

    dto::CreateQueryRequest request {
        .timestamp = _id,
        .collectionName = startKey.collectionName,
        .schemaName = startKey.schema->name,
        .key = startKey.storage.share(),
        .endKey = endKey.storage.share(),
        .recordLimit = recordLimit,
        .includeVersionMismatch = includeVersionMismatch,
        .reverseDirection = reverseDirection,
        .filterExpression = std::move(filter),
        .projection = std::move(projection)
    };

    return _client->_HTTPClient.POST<dto::CreateQueryRequest, dto::CreateQueryResponse>("/api/CreateQuery", std::move(request))
        .then([this] (auto&& futResp) {
          auto&& [status, resp] = futResp.get();
          return Response<dto::QueryRequest>(std::move(status), dto::QueryRequest{.timestamp=_id, .queryId=resp.queryId});
        });
}

boost::future<Response<dto::QueryResponse>> TxnHandle::query(dto::QueryRequest query) {
    return _client->_HTTPClient.POST<dto::QueryRequest, dto::QueryResponse>("/api/Query", std::move(query));
}

boost::future<Response<>> TxnHandle::endTxn(dto::EndAction endAction) {
    dto::TxnEndRequest request{
        .timestamp = _id,
        .action = endAction
    };

    return _client->_HTTPClient.POST<dto::TxnEndRequest>("/api/TxnEnd", std::move(request));
}
