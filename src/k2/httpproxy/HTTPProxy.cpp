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

// Indicates that the txn requested is no longer available
static const inline auto Txn_S410_Gone = sh::Statuses::S410_Gone("transaction does not exist");

void _shdRecToK2(shd::SKVRecord& shdrec, dto::SKVRecord& k2rec) {
    shdrec.visitRemainingFields([&k2rec](const auto&, auto&& value) mutable {
        if (value) {
            using T = typename std::remove_reference_t<decltype(value)>::value_type;
            if constexpr (std::is_same_v<T, shd::FieldType>) {
                auto kval = static_cast<dto::FieldType>(to_integral(*value));
                k2rec.serializeNext<dto::FieldType>(kval);
            } else if constexpr (std::is_same_v<T, sh::String>) {
                String k2str(value->data(), value->size());
                k2rec.serializeNext<String>(std::move(k2str));
            } else {
                k2rec.serializeNext<T>(*value);
            }
        } else {
            k2rec.serializeNull();
        }
    });
    k2rec.seekField(0);
}

template <typename T>
void _buildSHDRecordHelperVisitor(std::optional<T> value, String&, shd::SKVRecordBuilder& builder) {
    if (value) {
        if constexpr (std::is_same_v<T, dto::FieldType>) {
            auto ft = static_cast<shd::FieldType>(to_integral(*value));
            builder.serializeNext<shd::FieldType>(ft);
        } else if constexpr (std::is_same_v<T, String>) {
            sh::String str(value->data(), value->size());
            builder.serializeNext<sh::String>(std::move(str));
        } else {
            builder.serializeNext<T>(*value);
        }
    } else {
        builder.serializeNull();
    }
}

shd::SKVRecord _buildSHDRecord(dto::SKVRecord& k2rec, sh::String& collectionName, std::shared_ptr<shd::Schema> shdSchema) {
    shd::SKVRecordBuilder builder(collectionName, shdSchema);
    FOR_EACH_RECORD_FIELD(k2rec, _buildSHDRecordHelperVisitor, builder);
    return builder.build();
}

/*

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
seastar::future<std::tuple<sh::Status, shd::CollectionCreateResponse>>
HTTPProxy::_handleCreateCollection(shd::CollectionCreateRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create collection request {}", request);

    dto::CollectionMetadata meta{
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
            return MakeHTTPResponse<shd::CollectionCreateResponse>(sh::Status{.code=status.code, .message=status.message}, shd::CollectionCreateResponse{});
        });
}

seastar::future<std::tuple<sh::Status, shd::CreateSchemaResponse>>
HTTPProxy::_handleCreateSchema(shd::CreateSchemaRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create schema request {}", request);

    std::vector<dto::SchemaField> k2fields;

    for (auto& f: request.schema.fields) {
        k2fields.push_back(dto::SchemaField{
            .type = static_cast<dto::FieldType>(f.type),
            .name = f.name,
            .descending = f.descending,
            .nullLast = f.nullLast
        });
    }
    dto::Schema schema{
        .name = request.schema.name,
        .version = request.schema.version,
        .fields = std::move(k2fields),
        .partitionKeyFields = request.schema.partitionKeyFields,
        .rangeKeyFields = request.schema.rangeKeyFields
    };

    return _client.createSchema(String(request.collectionName), std::move(schema))
        .then([](auto&& result) {
            return MakeHTTPResponse<shd::CreateSchemaResponse>(sh::Status{.code=result.status.code, .message=result.status.message}, shd::CreateSchemaResponse{});
        });
}

seastar::future<std::tuple<sh::Status, shd::TxnBeginResponse>>
HTTPProxy::_handleTxnBegin(shd::TxnBeginRequest&& request){
    K2LOG_D(log::httpproxy, "Received begin txn request {}", request);
    K2TxnOptions opts{
        .deadline= Deadline<>(request.options.timeout),
        .priority = static_cast<dto::TxnPriority>(request.options.priority),
        .syncFinalize = request.options.syncFinalize
    };
    return _client.beginTxn(std::move(opts))
        .then([this](auto&& txn) {
            K2LOG_D(log::httpproxy, "begin txn: {}", txn.mtr());
            auto ts = txn.mtr().timestamp;
            shd::Timestamp shts{.endCount = ts.endCount, .tsoId = ts.tsoId, .startDelta = ts.startDelta};
            if (auto it = _txns.find(shts); it != _txns.end()) {
                return MakeHTTPResponse<shd::TxnBeginResponse>(sh::Statuses::S500_Internal_Server_Error("duplicate transaction ID detected"), shd::TxnBeginResponse{});
            }
            else {
                _txns.insert(it, {shts, ManagedTxn{.handle=std::move(txn), .queries={}}});
                return MakeHTTPResponse<shd::TxnBeginResponse>(sh::Statuses::S201_Created(""), shd::TxnBeginResponse{.timestamp=shts});
            }
        });
}

seastar::future<std::tuple<sh::Status, shd::WriteResponse>>
HTTPProxy::_handleWrite(shd::WriteRequest&& request) {
    K2LOG_D(log::httpproxy, "Received write request {}", request);

    return seastar::do_with(std::move(request), [this] (auto& request) {
        return _getSchemas(request.collectionName, request.schemaName, request.value.schemaVersion)
        .then([this, &request](auto&& schemas) mutable {
            auto& [status, k2Schema, shdSchema] = schemas;
            if (!status.is2xxOK()) {
                return MakeHTTPResponse<shd::WriteResponse>(sh::Status{.code=status.code, .message=status.message}, shd::WriteResponse{});
            }
            auto it = _txns.find(request.timestamp);
            if (it == _txns.end()) {
                return MakeHTTPResponse<shd::WriteResponse>(Txn_S410_Gone, shd::WriteResponse{});
            }
            dto::SKVRecord k2record(request.collectionName, k2Schema);
            shd::SKVRecord shdrecord(request.schemaName, shdSchema, std::move(request.value), true);
            _shdRecToK2(shdrecord, k2record);

            return it->second.handle.write(k2record, request.isDelete, static_cast<dto::ExistencePrecondition>(request.precondition))
                .then([](WriteResult&& result) {
                    return MakeHTTPResponse<shd::WriteResponse>(sh::Status{.code = result.status.code, .message = result.status.message}, shd::WriteResponse{});
                });
        });
    });
}

seastar::future<std::tuple<sh::Status, shd::ReadResponse>>
HTTPProxy::_handleRead(shd::ReadRequest&& request) {
    K2LOG_D(log::httpproxy, "Received read request {}", request);

    return seastar::do_with(std::move(request), [this](auto& request) {
        return _getSchemas(request.collectionName, request.schemaName, request.key.schemaVersion)
            .then([this, &request](auto&& schemas) mutable {
                auto& [status, k2Schema, shdSchema] = schemas;
                if (!status.is2xxOK()) {
                    return MakeHTTPResponse<shd::ReadResponse>(sh::Status{.code = status.code, .message = status.message}, shd::ReadResponse{});
                }
                auto it = _txns.find(request.timestamp);
                if (it == _txns.end()) {
                    return MakeHTTPResponse<shd::ReadResponse>(Txn_S410_Gone, shd::ReadResponse{});
                }
                dto::SKVRecord k2record(request.collectionName, k2Schema);
                shd::SKVRecord shdrecord(request.schemaName, shdSchema, std::move(request.key), true);
                _shdRecToK2(shdrecord, k2record);

                return it->second.handle.read(std::move(k2record))
                    .then([&request, shdSchema, k2Schema](auto&& result) {
                        if (!result.status.is2xxOK()) {
                            return MakeHTTPResponse<shd::ReadResponse>(sh::Status{.code = result.status.code, .message = result.status.message}, shd::ReadResponse{});
                        }
                        auto rec = _buildSHDRecord(result.value, request.collectionName, shdSchema);
                        shd::ReadResponse resp{
                            .collectionName=request.collectionName,
                            .schemaName=request.schemaName,
                            .record=std::move(rec.getStorage())
                        };
                        return MakeHTTPResponse<shd::ReadResponse>(sh::Statuses::S200_OK(""), std::move(resp));
                    });
            });
    });
}

seastar::future<std::tuple<sh::Status, shd::QueryResponse>>
HTTPProxy::_handleQuery(shd::QueryRequest&& request) {
    K2LOG_D(log::httpproxy, "Received query request {}", request);
    return MakeHTTPResponse<shd::QueryResponse>(sh::Statuses::S501_Not_Implemented("query not implemented"), shd::QueryResponse{});
}

seastar::future<std::tuple<sh::Status, shd::TxnEndResponse>>
HTTPProxy::_handleTxnEnd(shd::TxnEndRequest&& request) {
    K2LOG_D(log::httpproxy, "Received txn end request {}", request);
    auto it = _txns.find(request.timestamp);
    if (it == _txns.end()) {
        return MakeHTTPResponse<shd::TxnEndResponse>(Txn_S410_Gone, shd::TxnEndResponse{});
    }
    return it->second.handle.end(request.action == shd::EndAction::Commit)
        .then([this, &request] (auto&& result) {
            if (result.status.is2xxOK() || result.status.is4xxNonRetryable()) {
                _txns.erase(request.timestamp);
            }
            return MakeHTTPResponse<shd::TxnEndResponse>(sh::Status{.code=result.status.code, .message=result.status.message}, shd::TxnEndResponse{});
        });
}

seastar::future<std::tuple<sh::Status, shd::GetSchemaResponse>>
HTTPProxy::_handleGetSchema(shd::GetSchemaRequest&& request) {
    K2LOG_D(log::httpproxy, "Received get schema request {}", request);
    return MakeHTTPResponse<shd::GetSchemaResponse>(sh::Statuses::S501_Not_Implemented("get schema not implemented"), shd::GetSchemaResponse{});
}

seastar::future<std::tuple<sh::Status, shd::CreateQueryResponse>>
HTTPProxy::_handleCreateQuery(shd::CreateQueryRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create query request {}", request);
    return MakeHTTPResponse<shd::CreateQueryResponse>(sh::Statuses::S501_Not_Implemented("create query not implemented"), shd::CreateQueryResponse{});
}

seastar::future<std::tuple<Status, std::shared_ptr<dto::Schema>, std::shared_ptr<shd::Schema>>>
HTTPProxy::_getSchemas(sh::String cname, sh::String sname, int64_t sversion) {
    return _client.getSchema(cname, sname, sversion)
        .then([this, cname=std::move(cname)](GetSchemaResult&& result) mutable {
            if (!result.status.is2xxOK()) {
                return seastar::make_ready_future<std::tuple<Status, std::shared_ptr<dto::Schema>, std::shared_ptr<shd::Schema>>>(std::move(result.status), std::shared_ptr<dto::Schema>(), std::shared_ptr<shd::Schema>());
            }
            // create the nested maps as needed - we have a schema
            auto& shdSchemaPtr = _shdSchemas[cname][result.schema->name][result.schema->version];
            if (!shdSchemaPtr) {
                std::vector<shd::SchemaField> shdfields;

                for (auto& f : result.schema->fields) {
                    shdfields.push_back(shd::SchemaField{
                        .type = static_cast<shd::FieldType>(f.type),
                        .name = sh::String(f.name.data(), f.name.size()),
                        .descending = f.descending,
                        .nullLast = f.nullLast});
                }
                shd::Schema* schema  = new shd::Schema{
                    .name = result.schema->name,
                    .version = result.schema->version,
                    .fields = std::move(shdfields),
                    .partitionKeyFields = result.schema->partitionKeyFields,
                    .rangeKeyFields = result.schema->rangeKeyFields};
                shdSchemaPtr.reset(schema);
            }
            return seastar::make_ready_future<std::tuple<Status, std::shared_ptr<dto::Schema>, std::shared_ptr<shd::Schema>>>(std::move(result.status), std::move(result.schema), std::move(shdSchemaPtr));
        });
}

HTTPProxy::HTTPProxy() : _client(K23SIClientConfig()) {
}

seastar::future<> HTTPProxy::gracefulStop() {
    std::vector<seastar::future<>> futs;
    for (auto& [ts, txn]: _txns) {
        futs.push_back(txn.handle.kill());
    }
    return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result()
    .then([this] {
        _txns.clear();
    })
    .then([this] {
        return _client.gracefulStop();
    });
}

seastar::future<> HTTPProxy::start() {
    _registerMetrics();
    _registerAPI();
    return _client.start();
}

void HTTPProxy::_registerAPI() {
    K2LOG_I(log::httpproxy, "Registering HTTP API observers...");
    APIServer& api_server = AppBase().getDist<APIServer>().local();

    api_server.registerAPIObserver<sh::Statuses, shd::CollectionCreateRequest, shd::CollectionCreateResponse>
        ("CreateCollection", "create collection", [this] (auto&& request) {
            return _handleCreateCollection(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::CreateSchemaRequest, shd::CreateSchemaResponse>
        ("CreateSchema", "create schema", [this](auto&& request) {
            return _handleCreateSchema(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::TxnBeginRequest, shd::TxnBeginResponse>
        ("TxnBegin", "begin transaction", [this](auto&& request) {
            return _handleTxnBegin(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::WriteRequest, shd::WriteResponse>
        ("Write", "write request", [this](auto&& request) {
            return _handleWrite(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::ReadRequest, shd::ReadResponse>
        ("Read", "read request", [this](auto&& request) {
            return _handleRead(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::QueryRequest, shd::QueryResponse>
        ("Query", "query request", [this](auto&& request) {
            return _handleQuery(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::TxnEndRequest, shd::TxnEndResponse>
        ("TxnEnd", "end transaction", [this](auto&& request) {
            return _handleTxnEnd(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::CreateQueryRequest, shd::CreateQueryResponse>
        ("CreateQuery", "create query", [this](auto&& request) {
            return _handleCreateQuery(std::move(request));
        });

    api_server.registerAPIObserver<sh::Statuses, shd::GetSchemaRequest, shd::GetSchemaResponse>
        ("GetSchema", "get schema", [this](auto&& request) {
            return _handleGetSchema(std::move(request));
        });
}

void HTTPProxy::_registerMetrics() {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;

    _metric_groups.add_group("session",
    {
        sm::make_gauge("open_txns", [this]{ return  _txns.size();}, sm::description("Total number of open txn handles"), labels),
    });
}
}
