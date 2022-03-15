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

#include "Module.h"

#include <k2/appbase/AppEssentials.h>
#include <k2/common/Defer.h>
#include <k2/cpo/client/Heartbeat.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/infrastructure/APIServer.h>

namespace k2 {

// ********************** Validators
bool K23SIPartitionModule::_validateRetentionWindow(const dto::Timestamp& ts) const {
    bool result = ts.compareCertain(_retentionTimestamp) >= 0;
    K2LOG_D(log::skvsvr, "retention validation {}, {} vs {}",
            (result ? "passed" : "failed"), _retentionTimestamp, ts);
    return result;
}


template <typename T, typename = void>
struct has_key_field : std::false_type {};
template <typename T>
struct has_key_field<T, std::void_t<decltype(T::key)>>: std::true_type {};

template<typename RequestT>
bool K23SIPartitionModule::_validateRequestPartition(const RequestT& req) const {
    auto result = req.collectionName == _cmeta.name && req.pvid == _partition().keyRangeV.pvid;
    // validate partition owns the requests' key.
    // 1. common case assumes RequestT a Read request;
    // 2. now for the other cases, only Query request is implemented.
    if constexpr (std::is_same<RequestT, dto::K23SIQueryRequest>::value) {
        result = result && _partition.owns(req.key, req.reverseDirection);
    } else if constexpr(has_key_field<RequestT>::value) {
        result = result && _partition.owns(req.key);
    }
    else {
        result = result && _partition().keyRangeV.pvid == req.pvid;
    }
    K2LOG_D(log::skvsvr, "partition validation {}, for request={}", (result ? "passed" : "failed"), req);
    return result;
}

Status K23SIPartitionModule::_validateStaleWrite(const dto::K23SIWriteRequest& request, const Indexer::Iterator& iter) {
    if (!_validateRetentionWindow(request.mtr.timestamp)) {
        // the request is outside the retention window
        return dto::K23SIStatus::AbortRequestTooOld("write request is outside retention window");
    }

    auto ts = iter.getLastReadTime();
    if (request.mtr.timestamp.compareCertain(ts) < 0) {
        // this key range was read more recently than this write
        K2LOG_D(log::skvsvr, "observed history mutation detected for key: {}, transaction timestamp: {}, < observed key timestamp: {}", request.key, request.mtr.timestamp, ts);
        return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as this key (or key range) has been observed by another transaction.");
    }

    // check if we have a committed value newer than the request.
    // NB(1) if we try to place a WI over a committed value from different transaction with same ts.end,
    // reject the incoming write in order to avoid weird read-my-write problem for in-progress transactions
    // NB(2) we cannot allow writes past a committed value since a write has to imply a read causality, so
    // if a txn committed a value at time T5, then we must also assume they did a read at time T5
    // NB(3) This code does not care if there is a WI. If there is a WI, then this check can help avoid
    // an unnecessary PUSH.
    if (request.mtr.timestamp.compareCertain(iter.getLastCommittedTime()) <= 0) {
        // newest version is the latest committed and its newer than the request
        // or committed version from same transaction is found (e.g. bad retry on a write came through after commit)
        K2LOG_D(log::skvsvr, "failing write older than latest commit for key {}", request.key);
        return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as we have a newer committed write for this key from another transaction.");
    }
    // Note that we could also check the request id against the WI request id if it exists, and enforce
    // that it is non-decreasing. This would only catch a problem where: there is a bug in the client or
    // application code and the client does parallel writes to the same key. If the client wants to order
    // writes to the same key they must be done in serial.

    K2LOG_D(log::skvsvr, "stale write check passed for key {}", request.key);
    return dto::K23SIStatus::OK;
}

template <typename RequestT>
bool K23SIPartitionModule::_validateRequestPartitionKey(const RequestT& req) const {
    K2LOG_D(log::skvsvr, "Request: {}", req);

    if constexpr (std::is_same<RequestT, dto::K23SIQueryRequest>::value) {
        // Query is allowed to have empty partition key which means start or end of schema set
        return true;
    }
    else {
        return !req.key.partitionKey.empty();
    }
}

template <class RequestT>
Status K23SIPartitionModule::_validateReadRequest(const RequestT& request) const {
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return dto::K23SIStatus::RefreshCollection("collection refresh needed in read-type request");
    }
    if (!_validateRequestPartitionKey(request)) {
        // do not allow empty partition key
        return dto::K23SIStatus::BadParameter("missing partition key in read-type request");
    }
    if (!_validateRetentionWindow(request.mtr.timestamp)) {
        // the request is outside the retention window
        return dto::K23SIStatus::AbortRequestTooOld("request too old in read-type request");
    }
    if (_schemas.find(request.key.schemaName) == _schemas.end()) {
        // server does not have schema
        return dto::K23SIStatus::OperationNotAllowed("schema does not exist in read-type request");
    }

    return dto::K23SIStatus::OK;
}

Status K23SIPartitionModule::_validateWriteRequest(const dto::K23SIWriteRequest& request) {
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return dto::K23SIStatus::RefreshCollection("collection refresh needed in read-type request");
    }

    if (!_validateRequestPartitionKey(request)) {
        // do not allow empty partition key
        return dto::K23SIStatus::BadParameter("missing partition key in write");
    }

    auto schemaIt = _schemas.find(request.key.schemaName);
    if (schemaIt == _schemas.end()) {
        return dto::K23SIStatus::OperationNotAllowed("schema does not exist");
    }
    if (schemaIt->second.find(request.value.schemaVersion) == schemaIt->second.end()) {
        // server does not have schema
        return dto::K23SIStatus::OperationNotAllowed("schema version does not exist");
    }

    if (auto* twim = _twimMgr.getTxnWIMeta(request.mtr.timestamp); twim != nullptr) {
        if (twim->isAborted()) {
            return dto::K23SIStatus::AbortConflict("The transaction has been aborted");
        }
        else if (twim->isCommitted()) {
            return dto::K23SIStatus::BadParameter("The transaction has been committed");
        }
    }

    return dto::K23SIStatus::OK;
}
// ********************** Validators

K23SIPartitionModule::K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition,
                                           String cpoEndpoint) :
    _tsoClient(AppBase().getDist<tso::TSOClient>().local()),
    _cmeta(std::move(cmeta)),
    _partition(std::move(partition), _cmeta.hashScheme),
    _cpoEndpoint(std::move(cpoEndpoint)) {
    K2LOG_I(log::skvsvr, "ctor for cname={}, part={}", _cmeta.name, _partition);
}

seastar::future<> K23SIPartitionModule::_registerVerbs() {
    K2LOG_D(log::skvsvr, "Starting for partition: {}", _partition);

    APIServer& api_server = AppBase().getDist<APIServer>().local();
    cpo::HeartbeatResponder& hb_resp = AppBase().getDist<cpo::HeartbeatResponder>().local();
    hb_resp.setRoleMetadata("Partition assigned");

    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse>
    (dto::Verbs::K23SI_READ, [this, &hb_resp](dto::K23SIReadRequest&& request) {
        if (!hb_resp.isUp()) {
            return RPCResponse(dto::K23SIStatus::RefreshCollection("Heartbeat is dead"), dto::K23SIReadResponse{});
        }

        k2::OperationLatencyReporter reporter(_readLatency); // for reporting metrics
        return handleRead(std::move(request), FastDeadline(_config.readTimeout()), 0)
               .then([this, reporter=std::move(reporter)](auto&& response) mutable {
                    reporter.report();
                    return std::move(response);
               });
    });

    RPC().registerRPCObserver<dto::K23SIQueryRequest, dto::K23SIQueryResponse>
    (dto::Verbs::K23SI_QUERY, [this, &hb_resp](dto::K23SIQueryRequest&& request) {
        if (!hb_resp.isUp()) {
            return RPCResponse(dto::K23SIStatus::RefreshCollection("Heartbeat is dead"), dto::K23SIQueryResponse{});
        }

        k2::OperationLatencyReporter reporter(_queryPageLatency); // for reporting metrics
        return handleQuery(std::move(request), dto::K23SIQueryResponse{}, FastDeadline(_config.readTimeout()), 0)
                .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                    reporter.report();
                    return std::move(response);
               });
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest, dto::K23SIWriteResponse>
    (dto::Verbs::K23SI_WRITE, [this, &hb_resp](dto::K23SIWriteRequest&& request) {
        if (!hb_resp.isUp()) {
            return RPCResponse(dto::K23SIStatus::RefreshCollection("Heartbeat is dead"), dto::K23SIWriteResponse{});
        }

        k2::OperationLatencyReporter reporter(_writeLatency); // for reporting metrics
        return handleWrite(std::move(request), FastDeadline(_config.writeTimeout()))
            .then([this, reporter=std::move(reporter)] (auto&& resp) mutable {
                return _respondAfterFlush(std::move(resp))
                        .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                            reporter.report();
                            return std::move(response);
                        });
            });
    });

    RPC().registerRPCObserver<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
    (dto::Verbs::K23SI_TXN_PUSH, [this, &hb_resp](dto::K23SITxnPushRequest&& request) {
        if (!hb_resp.isUp()) {
            return RPCResponse(dto::K23SIStatus::RefreshCollection("Heartbeat is dead"), dto::K23SITxnPushResponse{});
        }

        k2::OperationLatencyReporter reporter(_pushLatency); // for reporting metrics
        return handleTxnPush(std::move(request))
                .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                    reporter.report();
                    return std::move(response);
               });
    });

    RPC().registerRPCObserver<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
    (dto::Verbs::K23SI_TXN_END, [this, &hb_resp](dto::K23SITxnEndRequest&& request) {
        if (!hb_resp.isUp()) {
            return RPCResponse(dto::K23SIStatus::RefreshCollection("Heartbeat is dead"), dto::K23SITxnEndResponse{});
        }

        return handleTxnEnd(std::move(request))
            .then([this] (auto&& resp) { return _respondAfterFlush(std::move(resp));});
    });

    RPC().registerRPCObserver<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
    (dto::Verbs::K23SI_TXN_HEARTBEAT, [this, &hb_resp](dto::K23SITxnHeartbeatRequest&& request) {
        if (!hb_resp.isUp()) {
            return RPCResponse(dto::K23SIStatus::RefreshCollection("Heartbeat is dead"), dto::K23SITxnHeartbeatResponse{});
        }

        return handleTxnHeartbeat(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
    (dto::Verbs::K23SI_TXN_FINALIZE, [this, &hb_resp](dto::K23SITxnFinalizeRequest&& request) {
        if (!hb_resp.isUp()) {
            return RPCResponse(dto::K23SIStatus::RefreshCollection("Heartbeat is dead"), dto::K23SITxnFinalizeResponse{});
        }

        return handleTxnFinalize(std::move(request))
                .then([this] (auto&& resp) {
                    return _respondAfterFlush(std::move(resp));
                });
    });

    RPC().registerRPCObserver<dto::K23SIPushSchemaRequest, dto::K23SIPushSchemaResponse>
    (dto::Verbs::K23SI_PUSH_SCHEMA, [this](dto::K23SIPushSchemaRequest&& request) {
        return handlePushSchema(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectRecordsRequest, dto::K23SIInspectRecordsResponse>
    (dto::Verbs::K23SI_INSPECT_RECORDS, [this](dto::K23SIInspectRecordsRequest&& request) {
        return handleInspectRecords(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectTxnRequest, dto::K23SIInspectTxnResponse>
    (dto::Verbs::K23SI_INSPECT_TXN, [this](dto::K23SIInspectTxnRequest&& request) {
        return handleInspectTxn(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectWIsRequest, dto::K23SIInspectWIsResponse>
    (dto::Verbs::K23SI_INSPECT_WIS, [this](dto::K23SIInspectWIsRequest&& request) {
        return handleInspectWIs(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectAllTxnsRequest, dto::K23SIInspectAllTxnsResponse>
    (dto::Verbs::K23SI_INSPECT_ALL_TXNS, [this](dto::K23SIInspectAllTxnsRequest&& request) {
        return handleInspectAllTxns(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectAllKeysRequest, dto::K23SIInspectAllKeysResponse>
    (dto::Verbs::K23SI_INSPECT_ALL_KEYS, [this](dto::K23SIInspectAllKeysRequest&& request) {
        return handleInspectAllKeys(std::move(request));
    });
    api_server.registerAPIObserver<dto::K23SIInspectAllKeysRequest, dto::K23SIInspectAllKeysResponse>
    ("InspectAllKeys", "Returns ALL keys on the partition", [this](dto::K23SIInspectAllKeysRequest&& request) {
        return handleInspectAllKeys(std::move(request));
    });

    return seastar::make_ready_future();
}

void K23SIPartitionModule::_unregisterVerbs() {
    APIServer& api_server = AppBase().getDist<APIServer>().local();

    RPC().registerMessageObserver(dto::Verbs::K23SI_READ, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_QUERY, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_WRITE, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_PUSH, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_END, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_HEARTBEAT, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_FINALIZE, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_PUSH_SCHEMA, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_RECORDS, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_TXN, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_WIS, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_ALL_TXNS, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_ALL_KEYS, nullptr);

    api_server.deregisterAPIObserver("InspectAllKeys");
}

void K23SIPartitionModule::_registerMetrics() {
    _metricGroups.clear();
    std::vector<sm::label_instance> labels;
    labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

    _metricGroups.add_group("Nodepool", {
        sm::make_gauge("indexer_keys",[this]{ return _indexer.size();},
                        sm::description("Number of keys in indexer"), labels),
        sm::make_counter("total_WI", _totalWI, sm::description("Number of WIs created"), labels),
        sm::make_counter("finalized_WI", _finalizedWI, sm::description("Number of WIs finalized"), labels),
        sm::make_gauge("record_versions", _recordVersions, sm::description("Number of record versions over all records"), labels),
        sm::make_counter("total_committed_payload", _totalCommittedPayload, sm::description("Total size of committed payloads"), labels),
        sm::make_histogram("read_latency", [this]{ return _readLatency.getHistogram();},
                sm::description("Latency of Read Operations"), labels),
        sm::make_histogram("write_latency", [this]{ return _writeLatency.getHistogram();},
                sm::description("Latency of Write Operations"), labels),
        sm::make_histogram("query_page_latency", [this]{ return _queryPageLatency.getHistogram();},
                sm::description("Latency of Query Page Operations"), labels),
        sm::make_histogram("push_latency", [this]{ return _pushLatency.getHistogram();},
                sm::description("Latency of Pushes"), labels),
        sm::make_histogram("query_page_scans", [this]{ return _queryPageScans.getHistogram();},
                sm::description("Number of records scanned by query page operations"), labels),
        sm::make_histogram("query_page_returns", [this]{ return _queryPageReturns.getHistogram();},
                sm::description("Number of records returned by query page operations"), labels)
    });
}

seastar::future<> K23SIPartitionModule::start() {

    _registerMetrics();

    K2LOG_I(log::skvsvr, "init cpo with {}", _cpoEndpoint);
    _cpo.init(_cpoEndpoint);
    if (_cmeta.retentionPeriod < _config.minimumRetentionPeriod()) {
        K2LOG_W(log::skvsvr,
            "Requested retention({}) is lower than minimum({}). Extending retention to minimum",
            _cmeta.retentionPeriod, _config.minimumRetentionPeriod());
        _cmeta.retentionPeriod = _config.minimumRetentionPeriod();
    }

    return _tsoClient.bootstrap(_cpoEndpoint)
    .then([this] () {
        return _tsoClient.getTimestamp();
    })
    .then([this](dto::Timestamp&& startTs) {
        K2LOG_D(log::skvsvr, "Starting timestamp: {}, retention period={}", startTs, _cmeta.retentionPeriod);
        _retentionTimestamp = startTs;

        _retentionUpdateTimer.setCallback([this] {
            K2LOG_D(log::skvsvr, "Partition {}, refreshing retention timestamp", _partition);
            return _tsoClient.getTimestamp()
                .then([this](dto::Timestamp&& ts) {
                    // set the retention timestamp (the time of the oldest entry we should keep)
                    _retentionTimestamp.maxEq(ts - _cmeta.retentionPeriod);
                    _txnMgr.updateRetentionTimestamp(_retentionTimestamp);
                    _twimMgr.updateRetentionTimestamp(_retentionTimestamp);
                });
        });
        _retentionUpdateTimer.armPeriodic(_config.retentionTimestampUpdateInterval());
        _persistence = std::make_shared<Persistence>();
        return _persistence->start()
            .then([this] {
                return _indexer.start(_retentionTimestamp);
            })
            .then([this] {
                return _twimMgr.start(_retentionTimestamp, _persistence, _cpoEndpoint);
            })
            .then([this] {
                return _txnMgr.start(_cmeta.name, _retentionTimestamp, _cmeta.heartbeatDeadline, _persistence, _cpoEndpoint);
            })
            .then([this] {
                return _recovery();
            })
            .then([this] {
                return _registerVerbs();
            });
    });
}

K23SIPartitionModule::~K23SIPartitionModule() {
    K2LOG_I(log::skvsvr, "dtor for cname={}, part={}", _cmeta.name, _partition);
}

seastar::future<> K23SIPartitionModule::gracefulStop() {
    K2LOG_I(log::skvsvr, "stop for cname={}, part={}", _cmeta.name, _partition);
    return _retentionUpdateTimer.stop()
        .then([this] {
            return _txnMgr.gracefulStop();
        })
        .then([this] {
            return _twimMgr.gracefulStop();
        })
        .then([this] {
            return _indexer.stop();
        })
        .then([this] {
            return _persistence->stop();
        })
        .then([this] {
            _unregisterVerbs();
            K2LOG_I(log::skvsvr, "stopped");
        });
}

// Helper for iterating over the indexer, modifies it to end() if iterator would go past the target schema
// or if it would go past begin() for reverse scan. Starting iterator must not be end() and must
// point to a record with the target schema
void K23SIPartitionModule::_scanAdvance(Indexer::Iterator& iter, const dto::K23SIQueryRequest& request) {
    if (!iter.atEnd()) {
        iter.next();
        iter.observeAt(request.mtr.timestamp);
    }
}

// Helper for handleQuery. Returns an iterator to start the scan at, accounting for
// desired schema and (eventually) reverse direction scan
Indexer::Iterator K23SIPartitionModule::_initializeScan(const dto::K23SIQueryRequest& request) {
    auto iter = _indexer.find(request.key, request.reverseDirection);
    iter.observeAt(request.mtr.timestamp);
    if (!iter.hasData() || // this key didn't exist in the indexer
        request.exclusiveKey) { // key is found, but we're asked to start with the next key in sequence
        _scanAdvance(iter, request);
    }
    return iter;
}

// Helper for handleQuery. Checks to see if the indexer scan should stop.
bool K23SIPartitionModule::_isScanDone(const Indexer::Iterator& iter, const dto::K23SIQueryRequest& request,
                                       size_t response_size) {
    // we're at end of iteration
    if (iter.atEnd()) {
        return true;
    }
    auto ikey = iter.getKey();

    if (ikey == request.key) {
        // Start key as inclusive overrides end key as exclusive
        return false;
    }
    if (!request.reverseDirection && ikey >= request.endKey &&
               request.endKey.partitionKey != "") {
        return true;
    }
    if (request.reverseDirection && ikey <= request.endKey) {
        return true;
    } else if (request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) {
        return true;
    } else if (response_size == _config.paginationLimit()) {
        return true;
    }

    return false;
}

// Helper for handleQuery. Returns continuation token (aka response.nextToScan)
dto::Key K23SIPartitionModule::_getContinuationToken(const Indexer::Iterator& iter,
                    const dto::K23SIQueryRequest& request, dto::K23SIQueryResponse& response, size_t response_size) {
    auto ikey = iter.getKey();
    // NB: In all checks below, iter.empty signifies that we're past the last
    // key available from the iterator.
    // Three cases where scan is for sure done:
    // 1. Record limit is reached
    // 2. Iterator is not end() but is >= user endKey
    // 3. Iterator is at end() and partition bounds contains endKey
    if ((request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) ||
        // Test for past user endKey:
        (!iter.atEnd() &&
            (request.reverseDirection ? ikey <= request.endKey : ikey >= request.endKey && request.endKey.partitionKey != "")) ||
        // Test for partition bounds contains endKey and we are at end()
        (iter.atEnd() &&
            (request.reverseDirection ?
            _partition().keyRangeV.startKey <= request.endKey.partitionKey :
            request.endKey.partitionKey <= _partition().keyRangeV.endKey && request.endKey.partitionKey != ""))) {
        return dto::Key();
    }
    else if (!iter.atEnd()) {
        // This is the paginated case
        response.exclusiveToken = false;
        return ikey;
    }

    // This is the multi-partition case
    if (request.reverseDirection) {
        response.exclusiveToken = true;
        return dto::Key {
            request.key.schemaName,
            _partition().keyRangeV.startKey,
            ""
        };
    } else {
        response.exclusiveToken = false;
        return dto::Key {
            request.key.schemaName,
            _partition().keyRangeV.endKey,
            ""
        };
    }
}

// Makes the SKVRecord and applies the request's filter to it. If the returned Status is not OK,
// the caller should return the status in the query response. Otherwise bool in tuple is whether
// the filter passed
std::tuple<Status, bool> K23SIPartitionModule::_doQueryFilter(dto::K23SIQueryRequest& request,
                                                              dto::SKVRecord::Storage& storage) {
    // We know the schema name exists because it is validated at the beginning of handleQuery
    auto schemaIt = _schemas.find(request.key.schemaName);
    auto versionIt = schemaIt->second.find(storage.schemaVersion);
    if (versionIt == schemaIt->second.end()) {
        return std::make_tuple(dto::K23SIStatus::OperationNotAllowed(
            "Schema version of found record does not exist"), false);
    }

    dto::SKVRecord record(request.collectionName, versionIt->second, storage.share(), true);
    bool keep = false;
    Status status = dto::K23SIStatus::OK;

    try {
        keep = request.filterExpression.evaluate(record);
    }
    catch(dto::NoFieldFoundException&) {}
    catch(dto::TypeMismatchException&) {}
    catch (dto::DeserializationError&) {
        status = dto::K23SIStatus::OperationNotAllowed("DeserializationError in query filter");
    }
    catch (dto::InvalidExpressionException&) {
        status = dto::K23SIStatus::OperationNotAllowed("InvalidExpression in query filter");
    }

    return std::make_tuple(std::move(status), keep);
}

seastar::future<std::tuple<Status, dto::K23SIQueryResponse>>
K23SIPartitionModule::handleQuery(dto::K23SIQueryRequest&& request, dto::K23SIQueryResponse&& response, FastDeadline deadline, uint32_t count) {
    K2LOG_D(log::skvsvr, "Partition: {}, received query {}", _partition, request);

    uint64_t numScans = 0;
    Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIQueryResponse{});
    }
    if (_partition.getHashScheme() != dto::HashScheme::Range) {
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("Query not implemented for hash partitioned collection"), dto::K23SIQueryResponse{});
    }

    auto iter = _initializeScan(request);
    for (; !_isScanDone(iter, request, response.results.size());
                        _scanAdvance(iter, request)) {
        ++numScans;
        auto [record, conflict] = iter.getDataRecordAt(request.mtr.timestamp);

        if (!record) {
            // happy case: we either had no versions, or all versions were newer than the requested timestamp
            continue;
        }

        // happy case: either committed, or txn is reading its own write
        if (!conflict) {
            if (!record->isTombstone) {
                auto [status, keep] = _doQueryFilter(request, record->value);
                if (!status.is2xxOK()) {
                    return RPCResponse(std::move(status), dto::K23SIQueryResponse{});
                }
                if (!keep) {
                    continue;
                }

                // apply projection if the user call addProjection
                if (request.projection.size() == 0) {
                    // want all fields
                    response.results.push_back(record->value.share());
                } else {
                    // serialize partial SKVRecord according to projection
                    dto::SKVRecord::Storage storage;
                    bool success = _makeProjection(record->value, request, storage);
                    if (!success) {
                        K2LOG_W(log::skvsvr, "Error making projection!");
                        return RPCResponse(dto::K23SIStatus::InternalError("Error making projection"),
                                                dto::K23SIQueryResponse{});
                    }

                    response.results.push_back(std::move(storage));
                }
            }

            continue;
        }

        // If we get here it is a conflict, first decide to push or return early
        if (response.results.size() >= _config.queryPushLimit()) {
            break;
        }
        auto ikey = iter.getKey();
        K2LOG_D(log::skvsvr, "query from txn {}, updates read cache for key range {} - {}",
                request.mtr, request.key, ikey);

        // TODO we can test the filter condition against the WI and last committed version and possibly
        // avoid a push

        K2LOG_D(log::skvsvr, "About to PUSH in query request");
        request.key = std::move(ikey);  // if we retry, do so with the key we're currently iterating on
        request.exclusiveKey=false; // re-process the key

        // add metrics
        _queryPageScans.add(numScans);
        _queryPageReturns.add(response.results.size());

        return _doPush(request.key, record->timestamp, request.mtr, deadline, ++count)
        .then([this, request=std::move(request),
                        resp=std::move(response), deadline, count](auto&& retryChallenger) mutable {
            if (!retryChallenger.is2xxOK()) {
                // sitting transaction won. Abort the incoming request
                return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in query push"), dto::K23SIQueryResponse{});
            }
            return handleQuery(std::move(request), std::move(resp), deadline, count);
        });
    }

    response.nextToScan = _getContinuationToken(iter, request, response, response.results.size());
    K2LOG_D(log::skvsvr, "nextToScan: {}, exclusiveToken: {}", response.nextToScan, response.exclusiveToken);

    _queryPageScans.add(numScans);
    _queryPageReturns.add(response.results.size());
    return RPCResponse(dto::K23SIStatus::OK("Query success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request, FastDeadline deadline, uint32_t count) {
    K2LOG_D(log::skvsvr, "Partition: {}, received read {}, count {}", _partition, request, count);

    Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIReadResponse{});
    }

    K2LOG_D(log::skvsvr, "read from txn {}, updates read cache for key {}",
                request.mtr, request.key);

    // find the record we should return
    auto iter = _indexer.find(request.key);
    iter.observeAt(request.mtr.timestamp);
    auto [rec, conflict] = iter.getDataRecordAt(request.mtr.timestamp);

    if (conflict) {
        // record is still pending and isn't from same transaction.
        return _doPush(request.key, rec->timestamp, request.mtr, deadline, ++count)
            .then([this, request=std::move(request), deadline, count](auto&& retryChallenger) mutable {
                if (!retryChallenger.is2xxOK()) {
                    return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in read push"), dto::K23SIReadResponse{});
                }
                return handleRead(std::move(request), deadline, count);
            });
    }

    if (rec == nullptr || rec->isTombstone) {
        // no version matches the incoming timestamp (or the version was a tombstone)
        return RPCResponse(dto::K23SIStatus::KeyNotFound("read did not find key"), dto::K23SIReadResponse{});
    }

    auto response = dto::K23SIReadResponse();
    response.value = rec->value.share();
    return RPCResponse(dto::K23SIStatus::OK("read succeeded"), std::move(response));
}

std::size_t K23SIPartitionModule::_findField(const dto::Schema schema, k2::String fieldName ,dto::FieldType fieldtype) {
    std::size_t fieldNumber = -1;
    for (std::size_t i = 0; i < schema.fields.size(); ++i) {
        if (schema.fields[i].name == fieldName && schema.fields[i].type == fieldtype) {
            return i;
        }
    }
    return fieldNumber;
}

template <typename T>
void _advancePayloadPosition(const dto::SchemaField& field, Payload& payload, bool& success) {
    (void) field;
    payload.skip<T>();
    success = true;
}

template <typename T>
void _copyPayloadBaseToUpdate(const dto::SchemaField& field, Payload& base, Payload& update, bool& success) {
    (void) field;
    success = base.copyToPayload<T>(update);
}

template <typename T>
void _getNextPayloadOffset(const dto::SchemaField& field, Payload& base, uint32_t baseCursor,
                           std::vector<uint32_t>& fieldsOffset, bool& success) {
    (void) field;
    (void) base;
    uint32_t tmpOffset = fieldsOffset[baseCursor] + sizeof(T);
    fieldsOffset.push_back(tmpOffset);
    success = true;
}

template <>
void _getNextPayloadOffset<String>(const dto::SchemaField& field, Payload& base, uint32_t baseCursor,
                           std::vector<uint32_t>& fieldsOffset, bool& success) {
    (void) field;
    uint32_t strLen;
    base.seek(fieldsOffset[baseCursor]);
    success = base.read(strLen);
    if (!success) return;
    uint32_t tmpOffset = fieldsOffset[baseCursor] + sizeof(uint32_t) + strLen; // uint32_t for length; '\0' doesn't count
    fieldsOffset.push_back(tmpOffset);
}

bool K23SIPartitionModule::_isUpdatedField(uint32_t fieldIdx, std::vector<uint32_t> fieldsForPartialUpdate) {
    for(std::size_t i = 0; i < fieldsForPartialUpdate.size(); ++i) {
        if (fieldIdx == fieldsForPartialUpdate[i]) return true;
    }
    return false;
}

bool K23SIPartitionModule::_makeFieldsForSameVersion(dto::Schema& schema, dto::K23SIWriteRequest& request, dto::DataRecord& version) {
    Defer defer([&version] {
        // make sure we always rewind the field data
        version.value.fieldData.seek(0);
    });
    Payload basePayload = version.value.fieldData.shareAll();   // base payload
    Payload payload(Payload::DefaultAllocator());                     // payload for new record

    for (std::size_t i = 0; i < schema.fields.size(); ++i) {
        if (_isUpdatedField(i, request.fieldsForPartialUpdate)) {
            // this field is updated
            if (request.value.excludedFields[i] == 0 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload has new value, AND
                // base payload also has this field (empty()==true indicate that base payload contains every fields).
                // Then use 'req' payload, at the mean time _advancePosition of base payload.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], request.value.fieldData, payload, success);
                if (!success) return false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], basePayload, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 0 &&
                    (!version.value.excludedFields.empty() && version.value.excludedFields[i] == 1)) {
                // Request's payload has new value, AND
                // base payload skipped this field.
                // Then use 'req' value, do not _advancePosition of base payload.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], request.value.fieldData, payload, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 1 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload skipped this value(means the field is updated to NULL), AND
                // base payload has this field.
                // Then exclude this field, at the mean time _advancePosition of base payload.
                request.value.excludedFields[i] = true;
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], basePayload, success);
                if (!success) return false;
            } else {
                // Request's payload skipped this value, AND base payload also skipped this field.
                // set excludedFields[i]
                request.value.excludedFields[i] = true;
            }
        } else {
            // this field is NOT updated
            if (request.value.excludedFields[i] == 0 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload contains this field, AND
                // base SKVRecord also has value of this field.
                // copy 'base skvRecord' value, at the mean time _advancePosition of 'req' payload.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                if (!success) return false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 0 &&
                    (!version.value.excludedFields.empty() && version.value.excludedFields[i] == 1)) {
                // Request's payload contains this field, AND
                // base SKVRecord do NOT has this field.
                // skip this field, at the mean time _advancePosition of 'req' payload.
                request.value.excludedFields[i] = true;
                bool success;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 1 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload do NOT contain this field, AND
                // base SKVRecord has value of this field.
                // copy 'base skvRecord' value.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                if (!success) return false;
                request.value.excludedFields[i] = false;
            } else {
                // else, request payload skipped this field, AND base SKVRecord also skipped this field,
                // set excludedFields[i]
                request.value.excludedFields[i] = true;
            }
        }
    }

    request.value.fieldData = std::move(payload);
    request.value.fieldData.truncateToCurrent();
    return true;
}

bool K23SIPartitionModule::_makeFieldsForDiffVersion(dto::Schema& schema, dto::Schema& baseSchema, dto::K23SIWriteRequest& request, dto::DataRecord& version) {
    Defer defer([&version] {
        // make sure we always rewind the field data
        version.value.fieldData.seek(0);
    });
    std::size_t findField; // find field index of base SKVRecord
    std::vector<uint32_t> fieldsOffset(1); // every fields offset of base SKVRecord
    std::size_t baseCursor = 0; // indicate fieldsOffset cursor

    Payload basePayload = version.value.fieldData.shareAll();   // base payload
    Payload payload(Payload::DefaultAllocator());                     // payload for new record

    // make every fields in schema for new full-record-WI
    for (std::size_t i = 0; i < schema.fields.size(); ++i) {
        findField = -1;
        if (!_isUpdatedField(i, request.fieldsForPartialUpdate)) {
            // if this field is NOT updated, payload value comes from base SKVRecord.
            findField = _findField(baseSchema, schema.fields[i].name, schema.fields[i].type);
            if (findField == (std::size_t)-1) {
                return false; // if do not find any field, Error return
            }

            // Each field's offset whose index is lower than baseCursor is save in the fieldsOffset
            if (findField < baseCursor) {
                if (request.value.excludedFields[i] == false) {
                    bool success;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                    if (!success) return false;
                }
                if (version.value.excludedFields.empty() || version.value.excludedFields[findField] == false) {
                    // copy value from base
                    basePayload.seek(fieldsOffset[findField]);
                    bool success = false;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                    if (!success) return false;
                    request.value.excludedFields[i] = false;
                } else {
                    // set excludedFields==true
                    request.value.excludedFields[i] = true;
                }
            } else {
                // 1. save offsets in 'fieldsOffset' from baseCursor to findField according to base SKVRecord;
                // note: add offset only if excludedField[i]==false.
                // 2. write 'findField' value from base SKVRecord to payload to make full-record-WI;
                // 3. baseCursor = findField + 1;
                for (; baseCursor <= findField; ++baseCursor) {
                    if (version.value.excludedFields.empty() || version.value.excludedFields[baseCursor] == false) {
                        bool success = false;
                        K2_DTO_CAST_APPLY_FIELD_VALUE(_getNextPayloadOffset, baseSchema.fields[baseCursor],
                                                      basePayload, baseCursor, fieldsOffset, success);
                        if (!success) return false;
                    } else {
                        fieldsOffset.push_back(fieldsOffset[baseCursor]);
                    }
                }

                if (request.value.excludedFields[i] == false) {
                    bool success;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                    if (!success) return false;
                }
                if (version.value.excludedFields.empty() || version.value.excludedFields[findField] == false) {
                    // copy value from base
                    basePayload.seek(fieldsOffset[findField]);
                    bool success = false;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                    if (!success) return false;
                    request.value.excludedFields[i] = false;
                } else {
                    // set excludedFields[i]=true
                    request.value.excludedFields[i] = true;
                }

                baseCursor = findField + 1;
            }
        } else {
            // this field is to be updated.
            if (request.value.excludedFields[i] == false) {
                // request's payload has a value.
                // 1. write() value from req's SKVRecord to payload
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], request.value.fieldData, payload, success);
                if (!success) return false;
            } else {
                // request's payload skips this field
                // 1. set excludedField
                request.value.excludedFields[i] = true;
            }
        }
    }

    request.value.fieldData = std::move(payload);
    request.value.fieldData.truncateToCurrent();
    return true;
}

bool K23SIPartitionModule::_parsePartialRecord(dto::K23SIWriteRequest& request, dto::DataRecord& previous) {
    // We already know the schema version exists because it is validated at the begin of handleWrite
    auto schemaIt = _schemas.find(request.key.schemaName);
    auto schemaVer = schemaIt->second.find(request.value.schemaVersion);
    dto::Schema& schema = *(schemaVer->second);

    if (!request.value.excludedFields.size()) {
        request.value.excludedFields = std::vector<bool>(schema.fields.size(), false);
    }

    // based on the latest version to construct the new SKVRecord
    if (request.value.schemaVersion == previous.value.schemaVersion) {
        // quick path --same schema version.
        // make every fields in schema for new SKVRecord
        if(!_makeFieldsForSameVersion(schema, request, previous)) {
            return false;
        }
    } else {
        // slow path --different schema version.
        auto latestSchemaVer = schemaIt->second.find(previous.value.schemaVersion);
        if (latestSchemaVer == schemaIt->second.end()) {
            return false;
        }
        dto::Schema& baseSchema = *(latestSchemaVer->second);

        if (!_makeFieldsForDiffVersion(schema, baseSchema, request, previous)) {
            return false;
        }
    }

    return true;
}

bool K23SIPartitionModule::_makeProjection(dto::SKVRecord::Storage& fullRec, dto::K23SIQueryRequest& request,
        dto::SKVRecord::Storage& projectionRec) {
    auto schemaIt = _schemas.find(request.key.schemaName);
    auto schemaVer = schemaIt->second.find(fullRec.schemaVersion);
    dto::Schema& schema = *(schemaVer->second);
    std::vector<bool> excludedFields(schema.fields.size(), true);   // excludedFields for projection
    Payload projectedPayload(Payload::DefaultAllocator());            // payload for projection

    for (uint32_t i = 0; i < schema.fields.size(); ++i) {
        if (fullRec.excludedFields.size() && fullRec.excludedFields[i]) {
            // A value of NULL in the record is treated the same as if the field doesn't exist in the record
            continue;
        }

        std::vector<k2::String>::iterator fieldIt;
        fieldIt = std::find(request.projection.begin(), request.projection.end(), schema.fields[i].name);
        if (fieldIt == request.projection.end()) {
            // advance base payload
            bool success = false;
            K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], fullRec.fieldData,
                                          success);
            if (!success) {
                fullRec.fieldData.seek(0);
                return false;
            }
            excludedFields[i] = true;
        } else {
            // write field value into payload
            bool success = false;
            K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], fullRec.fieldData,
                                          projectedPayload, success);
            if (!success) {
                fullRec.fieldData.seek(0);
                return false;
            }
            excludedFields[i] = false;
        }
    }

    // set cursor(0) of base payload
    fullRec.fieldData.seek(0);

    projectionRec.excludedFields = std::move(excludedFields);
    projectionRec.fieldData = std::move(projectedPayload);
    projectionRec.fieldData.truncateToCurrent();
    projectionRec.schemaVersion = fullRec.schemaVersion;
    return true;
}

template<typename ResponseT>
seastar::future<std::tuple<Status, ResponseT>>
K23SIPartitionModule::_respondAfterFlush(std::tuple<Status, ResponseT>&& resp) {
    K2LOG_D(log::skvsvr, "Awaiting persistence flush before responding");
    return _persistence->flush()
        .then([resp=std::move(resp)] (auto&& flushStatus) mutable {
            if (!flushStatus.is2xxOK()) {
                K2LOG_E(log::skvsvr, "Persistence failed with status {}", flushStatus);
                // TODO gracefully fail to aid in faster recovery.
                AppBase().stop(1);
            }

            K2LOG_D(log::skvsvr, "persistence flush succeeded. Sending response to client");
            return seastar::make_ready_future<std::tuple<Status, ResponseT>>(std::move(resp));
        });
}

seastar::future<Status>
K23SIPartitionModule::_designateTRH(dto::K23SI_MTR mtr, dto::Key trhKey) {
    K2LOG_D(log::skvsvr, "designating trh for {}", mtr);
    if (!_validateRetentionWindow(mtr.timestamp)) {
        return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortRequestTooOld("TRH create request is too old"));
    }

    return _txnMgr.createTxn(std::move(mtr), std::move(trhKey));
}

seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::handleWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline) {
    // NB: failures in processing a write do not require that we set the TR state to aborted at the TRH. We rely on
    //     the client to do the correct thing and issue an abort on a failure.
    K2LOG_D(log::skvsvr, "Partition: {}, handle write: {}", _partition, request);
    if (request.designateTRH) {
        if (!_validateRequestPartition(request)) {
            // tell client their collection partition is gone
            return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in write"), dto::K23SIWriteResponse());
        }
        return _designateTRH(request.mtr, request.key)
            .then([this, request=std::move(request), deadline] (auto&& status) mutable {
                if (!status.is2xxOK()) {
                    K2LOG_D(log::skvsvr, "failed creating TR for {}", request.mtr);
                    return RPCResponse(std::move(status), dto::K23SIWriteResponse{});
                }

                K2LOG_D(log::skvsvr, "succeeded creating TR. Processing write for {}", request.mtr);
                return _processWrite(std::move(request), deadline, 0);
            });
    }

    return _processWrite(std::move(request), deadline, 0);
}

seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::_processWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline, uint32_t count) {
    K2LOG_D(log::skvsvr, "processing write: {} with count {}", request, count);
    if (auto status = _validateWriteRequest(request); !status.is2xxOK()) {
        K2LOG_D(log::skvsvr, "rejecting write {} due to {}", request, status);
        // we may come here after a TRH create. Make sure to flush that
        return RPCResponse(std::move(status), dto::K23SIWriteResponse{});
    }
    auto iter = _indexer.find(request.key);
    if (auto status = _validateStaleWrite(request, iter); !status.is2xxOK()) {
        K2LOG_D(log::skvsvr, "rejecting write {} due to {}", request, status);
        // we may come here after a TRH create. Make sure to flush that
        return RPCResponse(std::move(status), dto::K23SIWriteResponse{});
    }

    // check to see if we should push
    auto* wi = iter.getWI();
    if (wi && wi->data.timestamp != request.mtr.timestamp) {
        // this is a write request finding a WI from a different transaction. Do a push with the remaining
        // deadline time.
        K2LOG_D(log::skvsvr, "different WI found for key {}", request.key);

        return _doPush(request.key, wi->data.timestamp, request.mtr, deadline, ++count)
            .then([this, request = std::move(request), deadline, count](auto&& retryChallenger) mutable {
                if (!retryChallenger.is2xxOK()) {
                    // challenger must fail. Flush in case a TR was created during this call to handle write
                    K2LOG_D(log::skvsvr, "write push challenger lost for key {}", request.key);
                    return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in write push"), dto::K23SIWriteResponse{});
                }

                K2LOG_D(log::skvsvr, "write push retry for key {} with count {}", request.key, count);
                return _processWrite(std::move(request), deadline, count);
            });
    }

    // Handle idempotency here. If request ids match, then this was a retry message from the client
    // and we should return OK
    if (wi &&
        request.mtr.timestamp == wi->data.timestamp &&
        request.request_id == wi->request_id) {
        K2LOG_D(log::skvsvr, "duplicate write encountered in request {}", request);
        return RPCResponse(dto::K23SIStatus::Created("wi was already created"), dto::K23SIWriteResponse{});
    }

    // Note that if we are here and a WI exists, it must be from the txn of the current request's transaction
    dto::DataRecord* head = iter.getLatestDataRecord();

    // Exists precondition can be set by the user (e.g. with a delete to know if a record was actually
    // delete) and it is set for partial updates
    if (request.precondition == dto::ExistencePrecondition::Exists && (!head || head->isTombstone)) {
        K2LOG_D(log::skvsvr, "Request {} not accepted since Exists precondition failed", request);
        iter.observeAt(request.mtr.timestamp);
        return RPCResponse(dto::K23SIStatus::ConditionFailed("Exists precondition failed"), dto::K23SIWriteResponse{});
    }

    if (request.precondition == dto::ExistencePrecondition::NotExists && head && !head->isTombstone) {
        // Need to add to read cache to prevent an erase coming in before this requests timestamp
        // If the condition passes (ie, there was no previous version and the insert succeeds) then
        // we do not need to insert into the read cache because the write intent will handle conflicts
        // and if the transaction aborts then any state it implicitly observes does not matter
        K2LOG_D(log::skvsvr, "write from txn {}, updates read cache for key {}", request.mtr, request.key);
        iter.observeAt(request.mtr.timestamp);

        // The ConditionFailed status does not mean that the transaction must abort. It is up to the user
        // to decide to abort or not, similar to a KeyNotFound status on read.
        return RPCResponse(dto::K23SIStatus::ConditionFailed("Previous record exists"), dto::K23SIWriteResponse{});
    }

    if (request.fieldsForPartialUpdate.size() > 0) {
        iter.observeAt(request.mtr.timestamp);

        if (!head) {
            return RPCResponse(dto::K23SIStatus::ConditionFailed("cannot apply partial update without previous version"), dto::K23SIWriteResponse{});
        }
        // parse the partial record to full record
        if (!_parsePartialRecord(request, *head)) {
            K2LOG_D(log::skvsvr, "can not parse partial record for key {}", request.key);
            return RPCResponse(dto::K23SIStatus::ConditionFailed("missing fields or can not interpret partialUpdate"), dto::K23SIWriteResponse{});
        }
    }

    // all checks passed - we're ready to place this WI as the latest version
    auto status = _createWI(std::move(request), iter);
    K2LOG_D(log::skvsvr, "WI creation with status {}", status);
    return RPCResponse(std::move(status), dto::K23SIWriteResponse{});
}

Status
K23SIPartitionModule::_createWI(dto::K23SIWriteRequest&& request, Indexer::Iterator& iter) {
    K2LOG_D(log::skvsvr, "Write Request creating WI: {}", request);
    // we need to copy this data into a new memory block so that we don't hold onto and fragment the transport memory
    dto::DataRecord rec{.value=request.value.copy(), .timestamp=request.mtr.timestamp, .isTombstone=request.isDelete};

    auto status = _twimMgr.addWrite(std::move(request.mtr), dto::Key(request.key), std::move(request.trh), std::move(request.trhCollection));

    if (!status.is2xxOK()) {
        return status;
    }

    // the TWIM accepted the write. Add it as a WI now
    iter.addWI(request.key, std::move(rec), request.request_id);
    _totalWI++;
    _persistence->append(iter.getWI()->data);
    return Statuses::S201_Created("WI created");
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
K23SIPartitionModule::handleTxnPush(dto::K23SITxnPushRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, push request: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in push"), dto::K23SITxnPushResponse());
    }
    if (!_validateRetentionWindow(request.challengerMTR.timestamp)) {
        // the request is outside the retention window
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request(challenger MTR) too old in push"), dto::K23SITxnPushResponse());
    }

    return _txnMgr.push(std::move(request.incumbentMTR), std::move(request.challengerMTR), std::move(request.key));
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
K23SIPartitionModule::handleTxnEnd(dto::K23SITxnEndRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, transaction end: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2LOG_D(log::skvsvr, "transaction end too old for txn={}", request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in end"), dto::K23SITxnEndResponse{});
    }

    return _txnMgr.endTxn(std::move(request));
}

seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
K23SIPartitionModule::handleTxnHeartbeat(dto::K23SITxnHeartbeatRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, transaction hb: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2LOG_D(log::skvsvr, "txn hb too old txn={}", request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in hb"), dto::K23SITxnHeartbeatResponse{});
    }
    if (!_validateRetentionWindow(request.mtr.timestamp)) {
        // the request is outside the retention window
        K2LOG_D(log::skvsvr, "txn hb too old txn={}", request.mtr);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("txn too old in hb"), dto::K23SITxnHeartbeatResponse{});
    }
    return _txnMgr.heartbeat(std::move(request.mtr), std::move(request.key))
        .then([](auto&& status) {
            return RPCResponse(std::move(status), dto::K23SITxnHeartbeatResponse{});
        });
}

seastar::future<Status>
K23SIPartitionModule::_doPush(dto::Key key, dto::Timestamp incumbentId, dto::K23SI_MTR challengerMTR, FastDeadline deadline, uint32_t count) {
    if (count > _config.maxPushCount()) {
        K2LOG_D(log::skvsvr, "Key {} has been pushed for {}, reject it", key, count);
        return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortConflict("request has already been pushed, abort due to multiple pushes"));
    }

    auto* incumbent = _twimMgr.getTxnWIMeta(incumbentId);
    K2ASSERT(log::skvsvr, incumbent != nullptr, "TWIM does not exists for {} in push for key {}", incumbentId, key)
    K2LOG_D(log::skvsvr, "executing push against txn={}, for mtr={}", *incumbent, challengerMTR);

    dto::K23SITxnPushRequest request{};
    request.collectionName = incumbent->trhCollection;
    request.incumbentMTR = incumbent->mtr;
    request.key = incumbent->trh; // this is the routing key - should be the TRH key
    request.challengerMTR = std::move(challengerMTR);
    return seastar::do_with(std::move(request), std::move(key), [this, deadline, &incumbent] (auto& request, auto& key) {
        auto fut = seastar::make_ready_future<std::tuple<Status, dto::K23SITxnPushResponse>>();
        if (incumbent->isAborted()) {
            fut = fut.then([] (auto&&) {
                return RPCResponse(dto::K23SIStatus::OK("challenger won in push since incumbent was already aborted"),
                              dto::K23SITxnPushResponse{ .incumbentFinalization = dto::EndAction::Abort,
                                                         .allowChallengerRetry = true});
            });
        }
        else if (incumbent->isCommitted()) {
            // Challenger should retry if they are newer than the committed value
            fut = fut.then([] (auto&&) {
                return RPCResponse(dto::K23SIStatus::OK("incumbent won in push since incumbent was already committed"),
                              dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Commit,
                                                        .allowChallengerRetry = true});
            });
        }
        else {
            // we don't know locally what's going on with this txn. Make a remote call to find out
            fut = fut.then([this, &request, deadline] (auto&&) {
                return _cpo.partitionRequest<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse, dto::Verbs::K23SI_TXN_PUSH>(deadline, request);
            });
        }
        return fut.then([this, &key, &request](auto&& responsePair) {
            auto& [status, response] = responsePair;
            K2LOG_D(log::skvsvr, "Push request completed with status={} and response={}", status, response);
            if (!status.is2xxOK()) {
                K2LOG_E(log::skvsvr, "txn push failed: {}", status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            // update the write intent if necessary
            auto iter = _indexer.find(key);
            if (!iter.hasData()) {
                return seastar::make_ready_future<Status>(response.allowChallengerRetry ? dto::K23SIStatus::OK : dto::K23SIStatus::AbortConflict);
            }

            auto* wi = iter.getWI();
            if (wi && wi->data.timestamp == request.incumbentMTR.timestamp) {
                switch (response.incumbentFinalization) {
                    case dto::EndAction::None: {
                        break;
                    }
                    case dto::EndAction::Abort: {
                        if (auto status = _twimMgr.abortWrite(request.incumbentMTR.timestamp, key); !status.is2xxOK()) {
                            K2LOG_W(log::skvsvr, "Unable to abort write in {} with local txn metadata due to {}", request.incumbentMTR, status);
                            return seastar::make_ready_future<Status>(std::move(status));
                        }
                        iter.abortWI();
                        _finalizedWI++;
                        break;
                    }
                    case dto::EndAction::Commit: {
                        if (auto status = _twimMgr.commitWrite(request.incumbentMTR.timestamp, key); !status.is2xxOK()) {
                            K2LOG_W(log::skvsvr, "Unable to commit write in {} with local txn metadata due to {}", request.incumbentMTR, status);
                            return seastar::make_ready_future<Status>(std::move(status));
                        }
                        _totalCommittedPayload += wi->data.value.fieldData.getSize();
                        iter.commitWI();
                        _recordVersions++;
                        _finalizedWI++;
                        break;
                    }
                    default:
                        K2LOG_E(log::skvsvr, "Unable to convert WI state based on txn state: {}, in txn: {}", response.incumbentFinalization, wi->data.timestamp);
                }
            }

            // signal the caller what to do with the challenger
            return seastar::make_ready_future<Status>(response.allowChallengerRetry ? dto::K23SIStatus::OK : dto::K23SIStatus::AbortConflict);
        });
    });
}

seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
K23SIPartitionModule::handleTxnFinalize(dto::K23SITxnFinalizeRequest&& request) {
    // find the version deque for the key
    K2LOG_D(log::skvsvr, "Partition: {}, txn finalize: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in finalize"), dto::K23SITxnFinalizeResponse{});
    }

    if (auto status = _twimMgr.endTxn(request.txnTimestamp, request.action); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to end transaction {} with local txn metadata due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    }

    // Put the twim in Finalizing state
    if (auto status=_twimMgr.finalizingWIs(request.txnTimestamp); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to start finalizing in transaction {} with local txn metadata due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    };

    if (auto status = _finalizeTxnWIs(request.txnTimestamp, request.action); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to finalize WIs in transaction {} due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    }

    // Finalize and discard the twim
    if (auto status=_twimMgr.finalizedTxn(request.txnTimestamp); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to complete finalization in transaction {} with local txn metadata due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    };

    return RPCResponse(dto::K23SIStatus::OK("Finalization success"), dto::K23SITxnFinalizeResponse{});
}

Status K23SIPartitionModule::_finalizeTxnWIs(dto::Timestamp txnts, dto::EndAction action) {
    auto* twim = _twimMgr.getTxnWIMeta(txnts);
    if (twim == nullptr) {
        return dto::K23SIStatus::KeyNotFound(fmt::format("Twim not found for txn {}", txnts));
    }
    K2ASSERT(log::skvsvr, twim->isCommitted() || twim->isAborted(), "Twim {} has not ended yet", *twim);
    for (auto& key: twim->writeKeys) {
        auto iter = _indexer.find(key);
        auto* wi = iter.getWI();
        K2ASSERT(log::skvsvr, wi!=nullptr,
                 "TWIM {} has registered WI for key{}, but key does not have a WI", *twim, key);
        K2ASSERT(log::skvsvr, wi->data.timestamp == txnts,
                 "TWIM {} has registered WI for key{}, but WI is from different transaction {}",
                 *twim, key, wi->data.timestamp);
        _finalizedWI++;

        switch (action) {
            case dto::EndAction::Abort: {
                K2LOG_D(log::skvsvr, "aborting {}, in txn {}", key, *twim);
                iter.abortWI();
                break;
            }
            case dto::EndAction::Commit: {
                K2LOG_D(log::skvsvr, "committing {}, in txn {}", key, *twim);
                _totalCommittedPayload += wi->data.value.fieldData.getSize();
                iter.commitWI();
                _recordVersions++;
                break;
            }
            default:
                K2LOG_W(log::skvsvr,
                        "failing finalize due to action mismatch key={}, action={}, twim={}",
                        key, action, *twim);
                return dto::K23SIStatus::OperationNotAllowed("request was not an abort or commit, likely memory corruption");
        }
    }

    return dto::K23SIStatus::OK;
}

seastar::future<std::tuple<Status, dto::K23SIPushSchemaResponse>>
K23SIPartitionModule::handlePushSchema(dto::K23SIPushSchemaRequest&& request) {
    K2LOG_D(log::skvsvr, "handlePushSchema for schema: {}", request.schema.name);
    if (_cmeta.name != request.collectionName) {
        return RPCResponse(Statuses::S403_Forbidden("Collection names in partition and request do not match"), dto::K23SIPushSchemaResponse{});
    }

    _schemas[request.schema.name][request.schema.version] = std::make_shared<dto::Schema>(request.schema);

    _indexer.createSchema(request.schema);
    return RPCResponse(Statuses::S200_OK("push schema success"), dto::K23SIPushSchemaResponse{});
}

// For test and debug purposes, not normal transaction processsing
// Returns all versions+WIs for a particular key
seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
K23SIPartitionModule::handleInspectRecords(dto::K23SIInspectRecordsRequest&& request) {
    K2LOG_D(log::skvsvr, "handleInspectRecords for: {}", request.key);

    auto iter = _indexer.find(request.key);

    if (!iter.hasData()) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound("Key not found in indexer"), dto::K23SIInspectRecordsResponse{});
    }

    dto::K23SIInspectRecordsResponse response{.records=iter.getAllDataRecords()};
    return RPCResponse(dto::K23SIStatus::OK("Inspect records success"), std::move(response));
}

// For test and debug purposes, not normal transaction processsing
// Returns the specified TRH
seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
K23SIPartitionModule::handleInspectTxn(dto::K23SIInspectTxnRequest&& request) {
    K2LOG_D(log::skvsvr, "handleInspectTxn {}", request);
    return _txnMgr.inspectTxn(request.timestamp);
}

// For test and debug purposes, not normal transaction processsing
// Returns all WIs on this node for all keys
seastar::future<std::tuple<Status, dto::K23SIInspectWIsResponse>>
K23SIPartitionModule::handleInspectWIs(dto::K23SIInspectWIsRequest&&) {
    K2LOG_D(log::skvsvr, "handleInspectWIs");
    std::vector<dto::WriteIntent> records;
    for (auto it=_twimMgr.twims().begin(); it != _twimMgr.twims().end(); ++it) {
        for (auto kit = it->second.writeKeys.begin(); kit != it->second.writeKeys.end(); ++kit) {
            auto iter = _indexer.find(*kit);
            auto* wi = iter.getWI();
            if (!wi) {
                continue;
            }
            dto::WriteIntent copy{
                .data = {
                    .value = wi->data.value.share(),
                    .timestamp = wi->data.timestamp,
                    .isTombstone = wi->data.isTombstone},
                    .request_id = wi->request_id};

            records.push_back(std::move(copy));
        }
    }

    dto::K23SIInspectWIsResponse response { std::move(records) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect WIs success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIInspectAllTxnsResponse>>
K23SIPartitionModule::handleInspectAllTxns(dto::K23SIInspectAllTxnsRequest&&) {
    K2LOG_D(log::skvsvr, "handleInspectAllTxns");
    return _txnMgr.inspectTxns();
}

// For test and debug purposes, not normal transaction processsing
// Returns all keys on this node
seastar::future<std::tuple<Status, dto::K23SIInspectAllKeysResponse>>
K23SIPartitionModule::handleInspectAllKeys(dto::K23SIInspectAllKeysRequest&& request) {
    (void) request;
    K2LOG_D(log::skvsvr, "handleInspectAllKeys");
    std::vector<dto::Key> keys;
    keys.reserve(_indexer.size());
    const auto& si = _indexer.getSchemaIndexer();
    for (auto it = si.cbegin(); it != si.cend(); ++it) {
        for (auto kit = it->second.impl.cbegin(); kit != it->second.impl.cend(); ++kit) {
            keys.push_back(dto::Key{.schemaName=it->first, .partitionKey=kit->first.partitionKey, .rangeKey=kit->first.rangeKey});
        }
    }

    dto::K23SIInspectAllKeysResponse response { std::move(keys) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect AllKeys success"), std::move(response));
}

seastar::future<> K23SIPartitionModule::_recovery() {
    //TODO perform recovery
    K2LOG_D(log::skvsvr, "Partition: {}, recovery", _partition);
    return seastar::make_ready_future();
}

}  // ns k2
