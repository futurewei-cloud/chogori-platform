/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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


#include <optional>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/infrastructure/APIServer.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/tso/client/Client.h>

#include <seastar/core/sleep.hh>

#include "Log.h"
namespace k2 {
using namespace dto;

const char* collname="HTTPClient";
k2::dto::Schema _schema {
    .name = "test_schema",
    .version = 1,
    .fields = std::vector<k2::dto::SchemaField> {
     {k2::dto::FieldType::STRING, "partitionKey", false, false},
     {k2::dto::FieldType::STRING, "rangeKey", false, false},
     {k2::dto::FieldType::STRING, "data", false, false}
    },
    .partitionKeyFields = std::vector<uint32_t> { 0 },
    .rangeKeyFields = std::vector<uint32_t> { 1 },
};
static thread_local std::shared_ptr<k2::dto::Schema> schemaPtr;

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

class HTTPClient {
public:  // application lifespan
    HTTPClient():
        _client(k2::K23SIClientConfig()) {
    }

    seastar::future<> gracefulStop() {
        _stopped = true;

        auto it = _txns.begin();
        for(; it != _txns.end(); ++it) {
            _endFuts.push_back(it->second.end(false).discard_result());
        }

        return seastar::when_all_succeed(_endFuts.begin(), _endFuts.end());
    }

    seastar::future<> start() {
        _stopped = false;
        _registerAPI();
        auto myid = seastar::this_shard_id();
        schemaPtr = std::make_shared<k2::dto::Schema>(_schema);
        auto _startFut = seastar::make_ready_future<>();
        _startFut = _startFut.then([this] {return _client.start();});
        if (myid == 0) {
            K2LOG_I(k2::log::httpclient, "Creating collection...");
            _startFut = _startFut.then([this] {
                k2::dto::CollectionMetadata meta {
                    .name = collname,
                    .hashScheme = dto::HashScheme::HashCRC32C,
                    .storageDriver = dto::StorageDriver::K23SI,
                    .capacity{
                        .dataCapacityMegaBytes = 0,
                        .readIOPs = 0,
                        .writeIOPs = 0,
                        .minNodes = _numPartitions()
                    },
                    .retentionPeriod = 5h,
                };
                return _client.makeCollection(std::move(meta))
                .then([this] (Status&& status) {
                    K2ASSERT(k2::log::httpclient, status.is2xxOK(), "Failed to create collection");
                    K2LOG_I(k2::log::httpclient, "Creating schema...");
                    return _client.createSchema(collname, _schema);
                }).discard_result();
            });
        }

        return _startFut;
    }

private:
    static void serializeRecordFromJSON(k2::SKVRecord& record, nlohmann::json&& jsonRecord) {
        for (const k2::dto::SchemaField& field : record.schema->fields) {
            std::string name = field.name;
            if (!jsonRecord.contains(name)) {
                record.serializeNull();
                continue;
            }

            K2_DTO_CAST_APPLY_FIELD_VALUE(serializeFieldFromJSON, field, record, jsonRecord);
        }
    }

    static nlohmann::json serializeJSONFromRecord(k2::SKVRecord& record) {
        nlohmann::json jsonRecord;
        for (const k2::dto::SchemaField& field : record.schema->fields) {
            K2_DTO_CAST_APPLY_FIELD_VALUE(serializeFieldFromRecord, field, record, jsonRecord);
        }
        return jsonRecord;
    }

    seastar::future<nlohmann::json> _handleBegin(nlohmann::json&& request) {
        (void) request;
        return _client.beginTxn(k2::K2TxnOptions())
        .then([this] (auto&& txn) {
            K2LOG_D(k2::log::httpclient, "begin txn: {}", txn.mtr());
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

    seastar::future<nlohmann::json> _handleEnd(nlohmann::json&& request) {
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

    seastar::future<nlohmann::json> _handleRead(nlohmann::json&& request) {
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

        return _client.getSchema(collectionName, schemaName, k2::K23SIClient::ANY_VERSION)
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

    seastar::future<nlohmann::json> _handleWrite(nlohmann::json&& request) {
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

    void _registerAPI() {
        K2LOG_I(k2::log::httpclient, "Registering HTTP API observers...");
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
    }

    void _registerMetrics() {
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

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _readLatency;
    k2::ExponentialHistogram _writeLatency;
    k2::ExponentialHistogram _txnLatency;
    k2::ExponentialHistogram _endLatency;

    uint64_t _totalTxns=0;
    uint64_t _abortedTxns=0;
    uint64_t _committedTxns=0;
    uint64_t _totalReads = 0;
    uint64_t _successReads = 0;
    uint64_t _failReads = 0;
    uint64_t _totalWrites = 0;
    uint64_t _successWrites = 0;
    uint64_t _failWrites = 0;

    bool _stopped = true;
    k2::K23SIClient _client;
    uint64_t _txnID = 0;
    std::unordered_map<uint64_t, k2::K2TxnHandle> _txns;
    std::vector<seastar::future<>> _endFuts;
    ConfigVar<uint32_t> _numPartitions{"num_partitions"};
};  // class HTTPClient

} // namespace k2

int main(int argc, char** argv) {
    k2::App app("K23SIBenchClient");
    app.addApplet<k2::APIServer>();
    app.addApplet<k2::tso::TSOClient>();
    app.addApplet<k2::HTTPClient>();
    app.addOptions()
        // config for dependencies
        ("num_partitions", bpo::value<uint32_t>(), "Number of k2 nodes to use for the collection")
        ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff");
    return app.start(argc, argv);
}
