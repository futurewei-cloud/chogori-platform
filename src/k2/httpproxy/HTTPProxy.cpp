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

#include <k2/infrastructure/APIServer.h>
#include <k2/common/Log.h>

namespace k2::log {
inline thread_local k2::logging::Logger httpproxy("k2::httpproxy");
}

namespace k2 {
using namespace dto;
namespace k2e = k2::dto::expression;

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
    if (value.has_value()) {
        jsonRecord[field.name] = *value;
    }
}

template <>
void serializeFieldFromRecord<k2::String>(const k2::SchemaField& field,
                                            k2::SKVRecord& record, nlohmann::json& jsonRecord) {
    std::optional<k2::String> value = record.deserializeNext<k2::String>();
    if (value.has_value()) {
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
    _registerAPI();
    auto _startFut = seastar::make_ready_future<>();
    _startFut = _startFut.then([this] {return _client.start();});
    return _startFut;
}

seastar::future<nlohmann::json> HTTPProxy::_handleBegin(nlohmann::json&& request) {
    (void) request;
    return _client.beginTxn(k2::K2TxnOptions())
    .then([this] (auto&& txn) {
        K2LOG_D(k2::log::httpproxy, "begin txn: {}", txn.mtr());
        _txns[_txnID++] = std::move(txn);
        nlohmann::json response;
        nlohmann::json status;
        status["message"] = "Begin txn success";
        status["code"] = 201;
        response["status"] = status;
        response["txnID"] = _txnID - 1;
        return seastar::make_ready_future<nlohmann::json>(std::move(response));
    });
}

seastar::future<nlohmann::json> HTTPProxy::_handleEnd(nlohmann::json&& request) {
    uint64_t id;
    bool commit;
    nlohmann::json response;
    try {
        request.at("txnID").get_to(id);
        request.at("commit").get_to(commit);
    } catch (...) {
        nlohmann::json status;
        status["message"] = "Bad json for end request";
        status["code"] = 400;
        response["status"] = status;
        return seastar::make_ready_future<nlohmann::json>(std::move(response));
    }

    std::unordered_map<uint64_t, k2::K2TxnHandle>::iterator it = _txns.find(id);
    if (it == _txns.end()) {
        nlohmann::json status;
        status["message"] = "Could not find txnID for end request";
        status["code"] = 400;
        response["status"] = status;
        return seastar::make_ready_future<nlohmann::json>(std::move(response));
    }

    return it->second.end(commit)
    .then([this, id] (k2::EndResult&& result) {
        nlohmann::json r;
        r["status"] = result.status;
        _txns.erase(id);
        return seastar::make_ready_future<nlohmann::json>(std::move(r));
    });
}

seastar::future<nlohmann::json> HTTPProxy::_handleRead(nlohmann::json&& request) {
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
        nlohmann::json status;
        status["message"] = "Bad json for read request";
        status["code"] = 400;
        response["status"] = status;
        return seastar::make_ready_future<nlohmann::json>(std::move(response));
    }

    std::unordered_map<uint64_t, k2::K2TxnHandle>::iterator it = _txns.find(id);
    if (it == _txns.end()) {
        nlohmann::json status;
        status["message"] = "Could not find txnID for read request";
        status["code"] = 400;
        response["status"] = status;
        return seastar::make_ready_future<nlohmann::json>(std::move(response));
    }

    return _client.getSchema(collectionName, schemaName, ANY_SCHEMA_VERSION)
    .then([this, id, collName=std::move(collectionName), jsonRecord=std::move(record)]
                                            (k2::GetSchemaResult&& result) mutable {
        if(!result.status.is2xxOK()) {
            nlohmann::json resp;
            resp["status"] = result.status;
            return seastar::make_ready_future<nlohmann::json>(std::move(resp));
        }

        k2::SKVRecord record = k2::SKVRecord(collName, result.schema);
        serializeRecordFromJSON(record, std::move(jsonRecord));

        return _txns[id].read(std::move(record))
        .then([] (k2::ReadResult<k2::dto::SKVRecord>&& result) {
            nlohmann::json resp;
            resp["status"] = result.status;

            if(!result.status.is2xxOK()) {
                return seastar::make_ready_future<nlohmann::json>(std::move(resp));
            }

            resp["record"] = serializeJSONFromRecord(result.value);
            return seastar::make_ready_future<nlohmann::json>(std::move(resp));
        });
    });
}

seastar::future<nlohmann::json> HTTPProxy::_handleWrite(nlohmann::json&& request) {
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
        nlohmann::json status;
        status["message"] = "Bad json for write request";
        status["code"] = 400;
        response["status"] = status;
        return seastar::make_ready_future<nlohmann::json>(std::move(response));
    }

    std::unordered_map<uint64_t, k2::K2TxnHandle>::iterator it = _txns.find(id);
    if (it == _txns.end()) {
        nlohmann::json status;
        status["message"] = "Could not find txnID for write request";
        status["code"] = 400;
        response["status"] = status;
        return seastar::make_ready_future<nlohmann::json>(std::move(response));
    }

    return _client.getSchema(collectionName, schemaName, schemaVersion)
    .then([this, id, collName=std::move(collectionName), jsonRecord=std::move(record)]
                                            (k2::GetSchemaResult&& result) mutable {
        if(!result.status.is2xxOK()) {
            nlohmann::json resp;
            resp["status"] = result.status;
            return seastar::make_ready_future<nlohmann::json>(std::move(resp));
        }

        k2::SKVRecord record = k2::SKVRecord(collName, result.schema);
        serializeRecordFromJSON(record, std::move(jsonRecord));

        return _txns[id].write(record)
        .then([] (k2::WriteResult&& result) {
            nlohmann::json resp;
            resp["status"] = result.status;
            return seastar::make_ready_future<nlohmann::json>(std::move(resp));
        });
    });
}

seastar::future<std::tuple<Status, CreateSchemaResponse>> HTTPProxy::_handleCreateSchema(
    CreateSchemaRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create schema request {}", request);
    return _client.createSchema(std::move(request.collectionName), std::move(request.schema))
        .then([] (CreateSchemaResult&& result) {
            return RPCResponse(std::move(result.status), CreateSchemaResponse{});
        });
}

seastar::future<std::tuple<Status, Schema>> HTTPProxy::_handleGetSchema(GetSchemaRequest&& request) {
    K2LOG_D(log::httpproxy, "Received get schema request {}", request);
    return _client.getSchema(std::move(request.collectionName), std::move(request.schemaName), request.schemaVersion)
    .then([](GetSchemaResult&& result) {
        return RPCResponse(std::move(result.status), result.schema ?  *result.schema : Schema{});
    });
}

seastar::future<std::tuple<Status, CollectionCreateResponse>> HTTPProxy::_handleCreateCollection(
    CollectionCreateRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create collection request {}", request);
    return _client.makeCollection(std::move(request.metadata), std::move(request.rangeEnds))
    .then([] (Status&& status) {
        return RPCResponse(std::move(status), CollectionCreateResponse());
    });
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
    if (!request.contains("fields"))
        return JsonResponse(Statuses::S400_Bad_Request("Invalid json"));

    for (auto& record : request["fields"]) {
        SchemaField field;
        record.at("type").get_to(field.type);
        String out;
        K2_DTO_CAST_APPLY_FIELD_VALUE(getEscapedString, field, record["value"], out);
        output += out;
    }
    return JsonResponse(Statuses::S200_OK(""), nlohmann::json{{"result", output}});
}

seastar::future<nlohmann::json> HTTPProxy::_handleCreateQuery(nlohmann::json&& jsonReq) {
    std::string collectionName;
    std::string schemaName;

    try {
        jsonReq.at("collectionName").get_to(collectionName);
        jsonReq.at("schemaName").get_to(schemaName);
    } catch(...) {
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
        if (req.contains("filter")) {
             k2e::Expression filter = req["filter"];
             result.query.setFilterExpression(std::move(filter));
        }
        if (req.contains("projection")) {
            std::vector<String> fieldNames = req.at("projection");
            result.query.addProjection(std::move(fieldNames));
        }
        _queries[_queryID++] = std::move(result.query);
        nlohmann::json resp{{"queryID", _queryID - 1}};
        return JsonResponse(std::move(result.status), std::move(resp));
    });
}

seastar::future<nlohmann::json> HTTPProxy::_handleQuery(nlohmann::json&& jsonReq) {
    uint64_t txnID;
    uint64_t queryID;

    try {
        jsonReq.at("txnID").get_to(txnID);
        jsonReq.at("queryID").get_to(queryID); 
    } catch(...) {
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

void HTTPProxy::_registerAPI() {
    K2LOG_I(k2::log::httpproxy, "Registering HTTP API observers...");
    k2::APIServer& api_server = k2::AppBase().getDist<k2::APIServer>().local();

    api_server.registerRawAPIObserver("BeginTxn", "Begin a txn, returning a numeric txn handle", [this](nlohmann::json&& request) {
        return _handleBegin(std::move(request));
    });
    api_server.registerRawAPIObserver("EndTxn", "End a txn", [this](nlohmann::json&& request) {
        return _handleEnd(std::move(request));
    });
    api_server.registerRawAPIObserver("Read", "handle read", [this](nlohmann::json&& request) {
        return _handleRead(std::move(request));
    });
    api_server.registerRawAPIObserver("Write", "handle write", [this](nlohmann::json&& request) {
        return _handleWrite(std::move(request));
    });
    api_server.registerRawAPIObserver("GetKeyString", "get range end", [this](nlohmann::json&& request) {
        return _handleGetKeyString(std::move(request));
    });
    api_server.registerRawAPIObserver("Query", "query", [this](nlohmann::json&& request) {
        return _handleQuery(std::move(request));
    });
    api_server.registerRawAPIObserver("CreateQuery", "create query", [this](nlohmann::json&& request) {
        return _handleCreateQuery(std::move(request));
    });


    api_server.registerAPIObserver<GetSchemaRequest, Schema>("GetSchema",
        "get schema",  [this] (GetSchemaRequest&& request) {
        return _handleGetSchema(std::move(request));
    });
    api_server.registerAPIObserver<CreateSchemaRequest, CreateSchemaResponse>("CreateSchema",
        "create schema",  [this] (CreateSchemaRequest&& request) {
        return _handleCreateSchema(std::move(request));
    });
    api_server.registerAPIObserver<CollectionCreateRequest, CollectionCreateResponse>("CreateCollection",
        "create collection",  [this] (CollectionCreateRequest&& request) {
        return _handleCreateCollection(std::move(request));
    });
}

void HTTPProxy::_registerMetrics() {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;
    labels.push_back(sm::label_instance("total_cores", seastar::smp::count));
    labels.push_back(sm::label_instance("active_cores", size_t(seastar::smp::count)));
    _metric_groups.add_group("session",
    {
        sm::make_counter("total_txns", _totalTxns, sm::description("Total number of transactions"), labels),
        sm::make_counter("aborted_txns", _abortedTxns, sm::description("Total number of aborted transactions"), labels),
        sm::make_counter("committed_txns", _committedTxns, sm::description("Total number of committed transactions"), labels),
        sm::make_counter("total_reads", _totalReads, sm::description("Total number of reads"), labels),
        sm::make_counter("success_reads", _successReads, sm::description("Total number of successful reads"), labels),
        sm::make_counter("fail_reads", _failReads, sm::description("Total number of failed reads"), labels),
        sm::make_counter("total_writes", _totalWrites, sm::description("Total number of writes"), labels),
        sm::make_counter("success_writes", _successWrites, sm::description("Total number of successful writes"), labels),
        sm::make_counter("fail_writes", _failWrites, sm::description("Total number of failed writes"), labels),
        sm::make_histogram("read_latency", [this]{ return _readLatency.getHistogram();}, sm::description("Latency of reads"), labels),
        sm::make_histogram("write_latency", [this]{ return _writeLatency.getHistogram();}, sm::description("Latency of writes"), labels),
        sm::make_histogram("txn_latency", [this]{ return _txnLatency.getHistogram();}, sm::description("Latency of entire txns"), labels),
        sm::make_histogram("txnend_latency", [this]{ return _endLatency.getHistogram();}, sm::description("Latency of txn end request"), labels)
    });
}
}