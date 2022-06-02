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

#include "HTTPProxy.h"

#include <seastar/core/sleep.hh>

#include <k2/logging/Log.h>

namespace k2::log {
inline thread_local k2::logging::Logger httpproxy("k2::httpproxy");
}

namespace k2 {
using namespace dto;
/*
template <typename T>
void serializeFieldFromJSON(const k2::SchemaField& field, k2::SKVRecord& record,
                                   const nlohmann::json& jsonRecord) {
    T value;
    jsonRecord.at(field.name).get_to(value);
    record.serializeNext<T>(value);
}

template <>
void serializeFieldFromJSON<k2::String>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, const nlohmann::json& jsonRecord) {
    std::string value;
    jsonRecord.at(field.name).get_to(value);
    record.serializeNext<k2::String>(value);
}

template <>
void serializeFieldFromJSON<std::decimal::decimal64>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, const nlohmann::json& jsonRecord) {
    (void) field;
    (void) record;
    (void) jsonRecord;
    throw k2::dto::TypeMismatchException("decimal64 type not supported with JSON interface");
}

template <>
void serializeFieldFromJSON<std::decimal::decimal128>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, const nlohmann::json& jsonRecord) {
    (void) field;
    (void) record;
    (void) jsonRecord;
    throw k2::dto::TypeMismatchException("decimal128 type not supported with JSON interface");
}

template <>
void serializeFieldFromJSON<k2::dto::FieldType>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, const nlohmann::json& jsonRecord) {
    (void) field;
    (void) record;
    (void) jsonRecord;
    throw k2::dto::TypeMismatchException("FieldType type not supported with JSON interface");
}

template <typename T>
void serializeFieldFromRecord(const k2::SchemaField& field, k2::SKVRecord& record,
                                   nlohmann::json& jsonRecord) {
    std::optional<T> value = record.deserializeNext<T>();
    if (!value.has_value()) {
        jsonRecord[field.name] = nullptr;
    } else {
        jsonRecord[field.name] = *value;
    }
}

template <>
void serializeFieldFromRecord<k2::String>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, nlohmann::json& jsonRecord) {
    std::optional<k2::String> value = record.deserializeNext<k2::String>();
    if (!value.has_value()) {
        jsonRecord[field.name] = nullptr;
    } else {
        std::string val_str = *value;
        jsonRecord[field.name] = val_str;
    }
}

template <>
void serializeFieldFromRecord<std::decimal::decimal64>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, nlohmann::json& jsonRecord) {
    (void) field;
    (void) record;
    (void) jsonRecord;
    throw k2::dto::TypeMismatchException("decimal64 type not supported with JSON interface");
}

template <>
void serializeFieldFromRecord<std::decimal::decimal128>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, nlohmann::json& jsonRecord) {
    (void) field;
    (void) record;
    (void) jsonRecord;
    throw k2::dto::TypeMismatchException("decimal128 type not supported with JSON interface");
}

template <>
void serializeFieldFromRecord<k2::dto::FieldType>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, nlohmann::json& jsonRecord) {
    (void) field;
    (void) record;
    (void) jsonRecord;
    throw k2::dto::TypeMismatchException("FieldType type not supported with JSON interface");
}


seastar::future<nlohmann::json> JsonResponse(Status&& status) {
    nlohmann::json resp;
    resp["status"] = std::move(status);
    return seastar::make_ready_future<nlohmann::json>(std::move(resp));
}

seastar::future<nlohmann::json> JsonResponse(Status&& status, nlohmann::json&& response) {
    nlohmann::json jsonResponse;
    jsonResponse["status"] = std::move(status);
    jsonResponse["response"] = std::move(response);
    return seastar::make_ready_future<nlohmann::json>(std::move(jsonResponse));
}

void HTTPProxy::serializeRecordFromJSON(k2::SKVRecord& record, nlohmann::json&& jsonRecord) {
    for (const k2::dto::SchemaField& field : record.schema->fields) {
        std::string name = field.name;
        if (!jsonRecord.contains(name)) {
            record.serializeNull();
            continue;
        }

        K2_DTO_CAST_APPLY_FIELD_VALUE(serializeFieldFromJSON, field, record, jsonRecord);
    }
}

nlohmann::json HTTPProxy::serializeJSONFromRecord(k2::SKVRecord& record) {
    nlohmann::json jsonRecord;
    for (const k2::dto::SchemaField& field : record.schema->fields) {
        K2_DTO_CAST_APPLY_FIELD_VALUE(serializeFieldFromRecord, field, record, jsonRecord);
    }
    return jsonRecord;
}

// Get FieldToKeyString value for a type
template <class T> void getEscapedString(const SchemaField& field, const nlohmann::json& jsonval,  String& out) {
    T val;
    (void)field;
    if constexpr  (std::is_same_v<T, std::decimal::decimal64>
        || std::is_same_v<T, std::decimal::decimal128>) {
        throw k2::dto::TypeMismatchException("decimal type not supported with JSON interface");
    } else {
        jsonval.get_to(val);
        out = FieldToKeyString<T>(val);
    }
}

// Convert [{type: "FieldType", value: "value"}, ..] to string that can be used in collection range end
seastar::future<nlohmann::json> HTTPProxy::_handleGetKeyString(nlohmann::json&& request) {
    String output;
    if (!request.contains("fields")) {
        _deserializationErrors++;
        return JsonResponse(Statuses::S400_Bad_Request("Invalid json"));
    }

    for (auto& record : request["fields"]) {
        SchemaField field;
        record.at("type").get_to(field.type);
        String out;
        K2_DTO_CAST_APPLY_FIELD_VALUE(getEscapedString, field, record["value"], out);
        output += out;
    }
    return JsonResponse(Statuses::S200_OK(""), nlohmann::json{{"result", output}});
}
*/



/*
seastar::future<HTTPPayload> HTTPProxy::_handleBegin(HTTPPayload&& request) {
    (void) request;
    return _client.beginTxn(k2::K2TxnOptions())
    .then([this] (auto&& txn) {
        K2LOG_D(k2::log::httpproxy, "begin txn: {}", txn.mtr());
        _txns[_txnID++] = std::move(txn);
        return JsonResponse(Statuses::S201_Created("Begin txn success"), nlohmann::json{{"txnID", _txnID - 1}});
    });
}

seastar::future<HTTPPayload> HTTPProxy::_handleEnd(HTTPPayload&& request) {
    uint64_t id;
    bool commit;
    nlohmann::json response;

    try {
        request.at("txnID").get_to(id);
        request.at("commit").get_to(commit);
    } catch (...) {
        _deserializationErrors++;
        return JsonResponse(Statuses::S400_Bad_Request("Bad json for end request"));
    }

    std::unordered_map<uint64_t, k2::K2TxnHandle>::iterator it = _txns.find(id);
    if (it == _txns.end()) {
        return JsonResponse(Statuses::S400_Bad_Request("Could not find txnID for end request"));
    }

    return it->second.end(commit)
    .then([this, id] (k2::EndResult&& result) {
        _txns.erase(id);
        return JsonResponse(std::move(result.status));
    });
}

seastar::future<HTTPPayload> HTTPProxy::_handleRead(HTTPPayload&& request) {
    std::string collectionName;
    std::string schemaName;
    uint64_t id;
    nlohmann::json record;
    nlohmann::json response;

    try {
        request.at("collectionName").get_to(collectionName);
        request.at("schemaName").get_to(schemaName);
        request.at("txnID").get_to(id);
        bool found = request.contains("record");
        if (!found) {
            throw std::runtime_error("Bad request");
        }
        record = request["record"];
    } catch (...) {
        _deserializationErrors++;
        return JsonResponse(Statuses::S400_Bad_Request("Invalid json for read request"));
    }

    std::unordered_map<uint64_t, k2::K2TxnHandle>::iterator it = _txns.find(id);
    if (it == _txns.end()) {
        return JsonResponse(Statuses::S400_Bad_Request("Could not find txnID for read request"));
    }

    return _client.getSchema(collectionName, schemaName, ANY_SCHEMA_VERSION)
    .then([this, id, collName=std::move(collectionName), jsonRecord=std::move(record)]
                                            (k2::GetSchemaResult&& result) mutable {
        if(!result.status.is2xxOK()) {
            return JsonResponse(std::move(result.status));
        }

        k2::SKVRecord record = k2::SKVRecord(collName, result.schema);
        try {
            serializeRecordFromJSON(record, std::move(jsonRecord));
        } catch(nlohmann::json::exception& e) {
            _deserializationErrors++;
            return JsonResponse(Statuses::S400_Bad_Request(e.what()));
        }
        return _txns[id].read(std::move(record))
        .then([this] (k2::ReadResult<k2::dto::SKVRecord>&& result) {
            if(!result.status.is2xxOK()) {
                return JsonResponse(std::move(result.status));
            }
            nlohmann::json resp;
            resp["record"] = serializeJSONFromRecord(result.value);
            return JsonResponse(std::move(result.status), std::move(resp));
        });
    });
}

seastar::future<HTTPPayload> HTTPProxy::_handleWrite(HTTPPayload&& request) {
    std::string collectionName;
    std::string schemaName;
    uint64_t id;
    uint64_t schemaVersion;
    nlohmann::json record;
    nlohmann::json response;

    try {
        request.at("collectionName").get_to(collectionName);
        request.at("schemaName").get_to(schemaName);
        request.at("txnID").get_to(id);
        request.at("schemaVersion").get_to(schemaVersion);
        bool found = request.contains("record");
        if (!found) {
            throw std::runtime_error("Bad request");
        }
        record = request["record"];
    } catch (...) {
        _deserializationErrors++;
        return JsonResponse(Statuses::S400_Bad_Request("Bad json for write request"));
    }

    std::unordered_map<uint64_t, k2::K2TxnHandle>::iterator it = _txns.find(id);
    if (it == _txns.end()) {
        return JsonResponse(Statuses::S400_Bad_Request("Could not find txnID for write request"));
    }

    return _client.getSchema(collectionName, schemaName, schemaVersion)
    .then([this, id, collName=std::move(collectionName), jsonRecord=std::move(record)]
                                            (k2::GetSchemaResult&& result) mutable {
        if(!result.status.is2xxOK()) {
            return JsonResponse(std::move(result.status));
        }

        k2::SKVRecord record = k2::SKVRecord(collName, result.schema);
        try {
            serializeRecordFromJSON(record, std::move(jsonRecord));
        } catch(nlohmann::json::exception& e) {
            _deserializationErrors++;
            return JsonResponse(Statuses::S400_Bad_Request(e.what()));
        }

        return _txns[id].write(record)
        .then([] (k2::WriteResult&& result) {
            return JsonResponse(std::move(result.status));
        });
    });
}


seastar::future<HTTPPayload> HTTPProxy::_handleCreateQuery(HTTPPayload&& jsonReq) {
    std::string collectionName;
    std::string schemaName;

    try {
        jsonReq.at("collectionName").get_to(collectionName);
        jsonReq.at("schemaName").get_to(schemaName);
    } catch(...) {
        _deserializationErrors++;
        return JsonResponse(Statuses::S400_Bad_Request("Bad json for query request"));
    }

    return _client.createQuery(collectionName, schemaName)
    .then([this, req=std::move(jsonReq)] (auto&& result) mutable {
        if(!result.status.is2xxOK()) {
            return JsonResponse(std::move(result.status));
        }
        K2LOG_D(log::httpproxy, "begin query {}", result);
        if (req.contains("startScanRecord")) {
            serializeRecordFromJSON(result.query.startScanRecord, std::move(req.at("startScanRecord")));
        }
        if (req.contains("endScanRecord")) {
            serializeRecordFromJSON(result.query.endScanRecord, std::move(req.at("endScanRecord")));
        }
        if (req.contains("limit")) {
            result.query.setLimit(req["limit"]);
        }
        if (req.contains("reverse")) {
            result.query.setReverseDirection(req["reverse"]);
        }
        _queries[_queryID++] = std::move(result.query);
        nlohmann::json resp{{"queryID", _queryID - 1}};
        return JsonResponse(std::move(result.status), std::move(resp));
    });
}

seastar::future<HTTPPayload> HTTPProxy::_handleQuery(HTTPPayload&& jsonReq) {
    uint64_t txnID;
    uint64_t queryID;

    try {
        jsonReq.at("txnID").get_to(txnID);
        jsonReq.at("queryID").get_to(queryID);
    } catch(...) {
        _deserializationErrors++;
        return JsonResponse(Statuses::S400_Bad_Request("Bad json for query request"));
    }
    auto txnIter = _txns.find(txnID);
    if (txnIter == _txns.end()) {
        return JsonResponse(Statuses::S400_Bad_Request("Could not find txnID for query request"));
    }
    auto queryIter = _queries.find(queryID);

    if (queryIter == _queries.end()) {
        return JsonResponse(Statuses::S400_Bad_Request("Could not find queryID for query request"));
    }

    return txnIter->second.query(queryIter->second)
    .then([this, queryID](QueryResult&& result) {
        if(!result.status.is2xxOK()) {
            return JsonResponse(std::move(result.status));
        }

        std::vector<nlohmann::json> records;
        records.reserve(result.records.size());
        for (auto& record: result.records) {
            records.push_back(serializeJSONFromRecord(record));
        }

        bool isDone = _queries[queryID].isDone();
        if (isDone) {
            _queries.erase(queryID);
        }
        nlohmann::json resp;
        resp["records"] = std::move(records);
        resp["done"] = isDone;
        return JsonResponse(std::move(result.status), std::move(resp));
    });
}

seastar::future<HTTPPayload> HTTPProxy::_handleGetSchema(HTTPPayload&& request) {
    K2LOG_D(log::httpproxy, "Received get schema request {}", request);
    return _client.getSchema(std::move(request.collectionName), std::move(request.schemaName), request.schemaVersion)
        .then([](GetSchemaResult&& result) {
            return RPCResponse(std::move(result.status), result.schema ? *result.schema : Schema{});
        });
}
*/
seastar::future<std::tuple<sh::Status, sh::dto::CollectionCreateResponse>>
HTTPProxy::_handleCreateCollection(sh::dto::CollectionCreateRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create collection request {}", request);

    k2::dto::CollectionMetadata meta{
        .name = request.metadata.name,
        .hashScheme = static_cast<dto::HashScheme>(request.metadata.hashScheme),
        .storageDriver = static_cast<dto::StorageDriver>(request.metadata.storageDriver),
        .capacity={
            .dataCapacityMegaBytes = request.metadata.capacity.dataCapacityMegaBytes,
            .readIOPs = request.metadata.capacity.readIOPs,
            .writeIOPs = request.metadata.capacity.writeIOPs,
            .minNodes = request.metadata.capacity.minNodes,
        },
        .retentionPeriod = request.metadata.retentionPeriod,
        .heartbeatDeadline = request.metadata.heartbeatDeadline,
        .deleted = request.metadata.deleted
    };

    std::vector<String> rends;
    for(auto& re: request.rangeEnds) {
        rends.push_back(re);
    }

    return _client.makeCollection(std::move(meta), std::move(rends))
        .then([](Status&& status) {
            return MakeHTTPResponse<sh::dto::CollectionCreateResponse>(sh::Status{.code=status.code, .message=status.message}, sh::dto::CollectionCreateResponse{});
        });
}

seastar::future<std::tuple<sh::Status, sh::dto::CreateSchemaResponse>>
HTTPProxy::_handleCreateSchema(sh::dto::CreateSchemaRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create schema request {}", request);

    std::vector<k2::dto::SchemaField> k2fields;

    for (auto& f: request.schema.fields) {
        k2fields.push_back(k2::dto::SchemaField{
            .type = static_cast<k2::dto::FieldType>(f.type),
            .name = f.name,
            .descending = f.descending,
            .nullLast = f.nullLast
        });
    }
    k2::dto::Schema schema{
        .name = request.schema.name,
        .version = request.schema.version,
        .fields = std::move(k2fields),
        .partitionKeyFields = request.schema.partitionKeyFields,
        .rangeKeyFields = request.schema.rangeKeyFields
    };

    return _client.createSchema(String(request.collectionName), std::move(schema))
        .then([](auto&& result) {
            return MakeHTTPResponse<sh::dto::CreateSchemaResponse>(sh::Status{.code=result.status.code, .message=result.status.message}, sh::dto::CreateSchemaResponse{});
        });
}

HTTPProxy::HTTPProxy():
        _client(k2::K23SIClientConfig()) {
}

seastar::future<> HTTPProxy::gracefulStop() {
    _stopped = true;

    auto it = _txns.begin();
    for(; it != _txns.end(); ++it) {
        _endFuts.push_back(it->second.end(false).discard_result());
    }

    return seastar::when_all_succeed(_endFuts.begin(), _endFuts.end());
}

seastar::future<> HTTPProxy::start() {
    _stopped = false;
    _registerMetrics();
    _registerAPI();
    auto _startFut = seastar::make_ready_future<>();
    _startFut = _startFut.then([this] {return _client.start();});
    return _startFut;
}

void HTTPProxy::_registerAPI() {
    K2LOG_I(k2::log::httpproxy, "Registering HTTP API observers...");
    k2::APIServer& api_server = k2::AppBase().getDist<k2::APIServer>().local();
/*
    api_server.registerRawAPIObserver("BeginTxn", "Begin a txn", [this](auto&& request) {
        return _handleBegin(std::move(request));
    });
    api_server.registerRawAPIObserver("EndTxn", "End a txn", [this](auto&& request) {
        return _handleEnd(std::move(request));
    });
    api_server.registerRawAPIObserver("Read", "handle read", [this](auto&& request) {
        return _handleRead(std::move(request));
    });
    api_server.registerRawAPIObserver("Write", "handle write", [this](auto&& request) {
        return _handleWrite(std::move(request));
    });
    api_server.registerRawAPIObserver("GetKeyString", "get range end", [this](auto&& request) {
        return _handleGetKeyString(std::move(request));
    });
    api_server.registerRawAPIObserver("Query", "query", [this](auto&& request) {
        return _handleQuery(std::move(request));
    });
    api_server.registerRawAPIObserver("CreateQuery", "create query", [this](auto&& request) {
        return _handleCreateQuery(std::move(request));
    });
    api_server.registerRawAPIObserver("GetSchema", "get schema",  [this] (auto&& request) {
        return _handleGetSchema(std::move(request));
    });
*/
    api_server.registerAPIObserver<sh::Statuses, sh::dto::CollectionCreateRequest, sh::dto::CollectionCreateResponse>
        ("CreateCollection", "create collection", [this] (auto&& request) {
            return _handleCreateCollection(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, sh::dto::CreateSchemaRequest, sh::dto::CreateSchemaResponse>("CreateSchema", "create schema", [this](auto&& request) {
        return _handleCreateSchema(std::move(request));
    });
}

void HTTPProxy::_registerMetrics() {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;

    _metric_groups.add_group("session",
    {
        sm::make_counter("deserialization_errors", _deserializationErrors, sm::description("Total number of deserialization errors"), labels),

        sm::make_gauge("open_txns", [this]{ return  _txns.size();}, sm::description("Total number of open txn handles"), labels),
        sm::make_gauge("open_queries", [this]{ return  _queries.size();}, sm::description("Total number of open queries"), labels),
    });
}
}
