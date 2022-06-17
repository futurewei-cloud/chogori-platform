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

shd::SKVRecord _buildSHDRecord(dto::SKVRecord& k2rec, const sh::String& collectionName, std::shared_ptr<shd::Schema> shdSchema) {
    shd::SKVRecordBuilder builder(collectionName, shdSchema);
    FOR_EACH_RECORD_FIELD(k2rec, _buildSHDRecordHelperVisitor, builder);
    return builder.build();
}

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
            shd::SKVRecord shdrecord(request.collectionName, shdSchema, std::move(request.value), true);
            try {
                _shdRecToK2(shdrecord, k2record);
            } catch(shd::DeserializationError& err) {
                return MakeHTTPResponse<shd::WriteResponse>(sh::Statuses::S400_Bad_Request(err.what()), shd::WriteResponse{});
            }

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
                shd::SKVRecord shdrecord(request.collectionName, shdSchema, std::move(request.key), true);
                try {
                    _shdRecToK2(shdrecord, k2record);
                }  catch(shd::DeserializationError& err) {
                    return MakeHTTPResponse<shd::ReadResponse>(sh::Statuses::S400_Bad_Request(err.what()), shd::ReadResponse{});
                }

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
    auto iter = _txns.find(request.timestamp);
    if (iter == _txns.end()) {
        return MakeHTTPResponse<shd::QueryResponse>(Txn_S410_Gone, shd::QueryResponse{});
    }
    auto queryIter = iter->second.queries.find(request.queryId);
    if (queryIter ==iter->second.queries.end()) {
        return MakeHTTPResponse<shd::QueryResponse>(sh::Statuses::S410_Gone("Query does not exist"), shd::QueryResponse{});
    }

    return iter->second.handle.query(queryIter->second)
    .then([this, request=std::move(request)](QueryResult&& result) {
        if(!result.status.is2xxOK()) {
            return MakeHTTPResponse<shd::QueryResponse>(sh::Status{.code = result.status.code, .message = result.status.message}, shd::QueryResponse{});
        }
        std::vector<shd::SKVRecord::Storage> records;
        records.reserve(result.records.size());
        for (auto& k2record: result.records) {
            sh::String collectionName(k2record.collectionName);
            // k2 schema is already populated by query api, get corresponding shd schema from cache
            auto shSChema = getSchemaFromCache(collectionName, k2record.schema);
            auto rec = _buildSHDRecord(k2record, collectionName, shSChema);
            records.push_back(std::move(rec.getStorage()));
        }
        auto& txnrec = _txns.at(request.timestamp);
        auto& query = txnrec.queries.at(request.queryId);
        bool isDone = query.isDone();
        if (isDone) {
            txnrec.queries.erase(request.queryId);
        }
        return MakeHTTPResponse<shd::QueryResponse>(sh::Status{.code=result.status.code, .message=result.status.message},
            shd::QueryResponse{.done = isDone, .records = std::move(records)});
    });
}

seastar::future<std::tuple<sh::Status, shd::TxnEndResponse>>
HTTPProxy::_handleTxnEnd(shd::TxnEndRequest&& request) {
    K2LOG_D(log::httpproxy, "Received txn end request {}", request);
    auto it = _txns.find(request.timestamp);
    if (it == _txns.end()) {
        return MakeHTTPResponse<shd::TxnEndResponse>(Txn_S410_Gone, shd::TxnEndResponse{});
    }
    return it->second.handle.end(request.action == shd::EndAction::Commit)
        .then([this, timestamp=request.timestamp] (auto&& result) {
            if (result.status.is2xxOK() || result.status.is4xxNonRetryable()) {
                _txns.erase(timestamp);
            }
            return MakeHTTPResponse<shd::TxnEndResponse>(sh::Status{.code=result.status.code, .message=result.status.message}, shd::TxnEndResponse{});
        });
}

seastar::future<std::tuple<sh::Status, shd::GetSchemaResponse>>
HTTPProxy::_handleGetSchema(shd::GetSchemaRequest&& request) {
    K2LOG_D(log::httpproxy, "Received get schema request {}", request);
    return _getSchemas(request.collectionName, request.schemaName, request.schemaVersion)
    .then([](auto&& schemas) {
        auto& [status, k2Schema, shdSchema] = schemas;
        return MakeHTTPResponse<shd::GetSchemaResponse>(
            sh::Status{.code=status.code, .message=status.message},
            status.is2xxOK() ? shd::GetSchemaResponse{.schema=*shdSchema}: shd::GetSchemaResponse{});
    });
}

void HTTPProxy::setQueryRecord(const sh::String& collectionName, shd::SKVRecord::Storage&& key, dto::SKVRecord& k2record) {
    if (key.fieldData.size() == 0) return;
    auto shdSchema = getSchemaFromCache(collectionName, k2record.schema);
    shd::SKVRecord shdrecord(collectionName, shdSchema, std::move(key), true);
    _shdRecToK2(shdrecord, k2record);
}

namespace dtoexp = dto::expression;
namespace shdexp = shd::expression;

dtoexp::Value getValue(shdexp::Value&& shval) {
    dtoexp::Value k2val;
    if (shval.isReference()) {
        k2val = dtoexp::makeValueReference(shval.fieldName);
    } else if (shval.type == shd::FieldType::NULL_T || shval.type == shd::FieldType::NOT_KNOWN || shval.type == shd::FieldType::NULL_LAST) {
        k2val.type =  static_cast<dto::FieldType>(to_integral(shval.type));
    } else {
        shd::applyTyped(shval, [&k2val](const auto& afr) {
            using T = shd::applied_type_t<decltype(afr)>;
            auto obj = afr.field.template get<T>();
            if constexpr (std::is_same_v<T, shd::FieldType>) {
                k2val = dtoexp::makeValueLiteral(static_cast<dto::FieldType>(to_integral(obj)));
            } else if constexpr (std::is_same_v<T, sh::String>) {
              k2val = dtoexp::makeValueLiteral<String>(String(std::move(obj)));
            } else {
              k2val = dtoexp::makeValueLiteral<T>(std::move(obj));
            }
        });
    }
    return k2val;
}

dtoexp::Expression getFilterExpression(shdexp::Expression&& shExpr) {
    std::vector<dtoexp::Value> values;
    values.reserve(shExpr.valueChildren.size());
    for (auto& val: shExpr.valueChildren) {
        values.push_back(getValue(std::move(val)));
    }
    std::vector<dtoexp::Expression> exprs;
    exprs.reserve(shExpr.expressionChildren.size());
    for (auto& cexpr: shExpr.expressionChildren) {
        exprs.push_back(getFilterExpression(std::move(cexpr)));
    }
    return dtoexp::makeExpression(static_cast<dtoexp::Operation>(to_integral(shExpr.op)), std::move(values), std::move(exprs));
}

seastar::future<std::tuple<sh::Status, shd::CreateQueryResponse>>
HTTPProxy::_handleCreateQuery(shd::CreateQueryRequest&& request) {
    K2LOG_D(log::httpproxy, "Received create query request {}", request);
    auto it = _txns.find(request.timestamp);
    if (it == _txns.end()) {
        return MakeHTTPResponse<shd::CreateQueryResponse>(Txn_S410_Gone, shd::CreateQueryResponse{});
    }
    return _client.createQuery(request.collectionName, request.schemaName)
        .then([this, req=std::move(request)] (auto&& result) mutable {
            if(!result.status.is2xxOK()) {
                return MakeHTTPResponse<shd::CreateQueryResponse>(sh::Status{.code = result.status.code, .message = result.status.message}, shd::CreateQueryResponse{});
            }
            try {
                setQueryRecord(req.collectionName, std::move(req.key), result.query.startScanRecord);
                setQueryRecord(req.collectionName, std::move(req.endKey), result.query.endScanRecord);

                if (req.recordLimit >= 0) result.query.setLimit(req.recordLimit);
                if (req.includeVersionMismatch) result.query.setIncludeVersionMismatch(req.includeVersionMismatch);
                if (req.reverseDirection) result.query.setReverseDirection(req.reverseDirection);
                if (req.filterExpression.op != shdexp::Operation::UNKNOWN) {
                    dtoexp::Expression expr = getFilterExpression(std::move(req.filterExpression));
                    result.query.setFilterExpression(std::move(expr));
                }
                if (req.projection.size() > 0) {
                    std::vector<String> projection;
                    projection.reserve(req.projection.size());
                    for (sh::String& p : req.projection) {
                        projection.push_back(String(std::move(p)));
                    }
                    result.query.addProjection(projection);
                }
            } catch(shd::DeserializationError& err) {
                return MakeHTTPResponse<shd::CreateQueryResponse>(sh::Statuses::S400_Bad_Request(err.what()), shd::CreateQueryResponse{});
            }

            auto queryId = _queryID++;
            _txns.at(req.timestamp).queries[queryId] = std::move(result.query);
            return MakeHTTPResponse<shd::CreateQueryResponse>(
                sh::Status{.code = result.status.code, .message = result.status.message},
                shd::CreateQueryResponse{.queryId = queryId});
        });
 }

// Get shd schema from k2 schema either from cache or convert
std::shared_ptr<shd::Schema> HTTPProxy::getSchemaFromCache(const sh::String& cname, std::shared_ptr<dto::Schema> schema) {
    // create the nested maps as needed - we have a schema
    auto& shdSchemaPtr = _shdSchemas[cname][schema->name][schema->version];
    if (!shdSchemaPtr) {
        std::vector<shd::SchemaField> shdfields;

        for (auto& f : schema->fields) {
            shdfields.push_back(shd::SchemaField{
                .type = static_cast<shd::FieldType>(f.type),
                .name = sh::String(f.name.data(), f.name.size()),
                .descending = f.descending,
                .nullLast = f.nullLast});
        }
        shd::Schema* shdSchema  = new shd::Schema{
            .name = schema->name,
            .version = schema->version,
            .fields = std::move(shdfields),
            .partitionKeyFields = schema->partitionKeyFields,
            .rangeKeyFields = schema->rangeKeyFields};
        shdSchemaPtr.reset(shdSchema);
    }
    return shdSchemaPtr;
}

seastar::future<std::tuple<Status, std::shared_ptr<dto::Schema>, std::shared_ptr<shd::Schema>>>
HTTPProxy::_getSchemas(sh::String cname, sh::String sname, int64_t sversion) {
    return _client.getSchema(cname, sname, sversion)
        .then([this, cname=std::move(cname)](GetSchemaResult&& result) mutable {
            if (!result.status.is2xxOK()) {
                return seastar::make_ready_future<std::tuple<Status, std::shared_ptr<dto::Schema>, std::shared_ptr<shd::Schema>>>(std::move(result.status), std::shared_ptr<dto::Schema>(), std::shared_ptr<shd::Schema>());
            }
            auto shdSchemaPtr = getSchemaFromCache(std::move(cname), result.schema);
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
