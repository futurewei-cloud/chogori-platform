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
#include <k2/dto/MessageVerbs.h>
#include <k2/infrastructure/APIServer.h>

namespace k2 {
namespace dto {
    // we want the read cache to determine ordering based on certain comparison so that we have some stable
    // ordering even across different nodes and TSOs
    const Timestamp& max(const Timestamp& a, const Timestamp& b) {
        return a.compareCertain(b) == Timestamp::LT ? b : a;
    }
} // ns dto

K23SIPartitionModule::K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition) :
    _cmeta(std::move(cmeta)),
    _partition(std::move(partition), _cmeta.hashScheme),
    _retentionUpdateTimer([this] {
        K2LOG_D(log::skvsvr, "Partition {}, refreshing retention timestamp", _partition);
        _retentionRefresh = _retentionRefresh.then([this]{
            return getTimeNow();
        })
        .then([this](dto::Timestamp&& ts) {
            // set the retention timestamp (the time of the oldest entry we should keep)
            _retentionTimestamp = ts - _cmeta.retentionPeriod;
            _txnMgr.updateRetentionTimestamp(_retentionTimestamp);
        })
        .finally([this]{
            _retentionUpdateTimer.arm(_config.retentionTimestampUpdateInterval());
        });
    }),
    _cpo(_config.cpoEndpoint()) {
    K2LOG_I(log::skvsvr, "ctor for cname={}, part={}", _cmeta.name, _partition);
}

seastar::future<> K23SIPartitionModule::start() {
    K2LOG_D(log::skvsvr, "Starting for partition: {}", _partition);

    APIServer& api_server = AppBase().getDist<APIServer>().local();

    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse>
    (dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
        return handleRead(std::move(request), FastDeadline(_config.readTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SIQueryRequest, dto::K23SIQueryResponse>
    (dto::Verbs::K23SI_QUERY, [this](dto::K23SIQueryRequest&& request) {
        return handleQuery(std::move(request), dto::K23SIQueryResponse{}, FastDeadline(_config.readTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest, dto::K23SIWriteResponse>
    (dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest&& request) {
        return handleWrite(std::move(request), FastDeadline(_config.writeTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
    (dto::Verbs::K23SI_TXN_PUSH, [this](dto::K23SITxnPushRequest&& request) {
        return handleTxnPush(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
    (dto::Verbs::K23SI_TXN_END, [this](dto::K23SITxnEndRequest&& request) {
        return handleTxnEnd(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
    (dto::Verbs::K23SI_TXN_HEARTBEAT, [this](dto::K23SITxnHeartbeatRequest&& request) {
        return handleTxnHeartbeat(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
    (dto::Verbs::K23SI_TXN_FINALIZE, [this](dto::K23SITxnFinalizeRequest&& request) {
        return handleTxnFinalize(std::move(request));
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


    if (_cmeta.retentionPeriod < _config.minimumRetentionPeriod()) {
        K2LOG_W(log::skvsvr,
            "Requested retention({}) is lower than minimum({}). Extending retention to minimum",
            _cmeta.retentionPeriod, _config.minimumRetentionPeriod());
        _cmeta.retentionPeriod = _config.minimumRetentionPeriod();
    }

    // todo call TSO to get a timestamp
    return getTimeNow()
        .then([this](dto::Timestamp&& watermark) {
            K2LOG_D(log::skvsvr, "Cache watermark: {}, period={}", watermark, _cmeta.retentionPeriod);
            _retentionTimestamp = watermark - _cmeta.retentionPeriod;
            _readCache = std::make_unique<ReadCache<dto::Key, dto::Timestamp>>(watermark, _config.readCacheSize());
            _retentionUpdateTimer.arm(_config.retentionTimestampUpdateInterval());
            return seastar::when_all_succeed(_recovery(), _txnMgr.start(_cmeta.name, _retentionTimestamp, _cmeta.heartbeatDeadline)).discard_result();
        });
}

K23SIPartitionModule::~K23SIPartitionModule() {
    K2LOG_I(log::skvsvr, "dtor for cname={}, part={}", _cmeta.name, _partition);
}

seastar::future<> K23SIPartitionModule::_recovery() {
    //TODO perform recovery
    K2LOG_D(log::skvsvr, "Partition: {}, recovery", _partition);
    return _persistence.makeCall(dto::K23SI_PersistenceRecoveryRequest{}, _config.persistenceTimeout());
}

seastar::future<> K23SIPartitionModule::gracefulStop() {
    K2LOG_I(log::skvsvr, "stop for cname={}, part={}", _cmeta.name, _partition);
    _retentionUpdateTimer.cancel();
    return seastar::when_all_succeed(std::move(_retentionRefresh), _txnMgr.gracefulStop()).discard_result().then([]{K2LOG_I(log::skvsvr, "stopped");});
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
_makeReadOK(dto::DataRecord* rec) {
    if (rec == nullptr || rec->isTombstone) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound("read did not find key"), dto::K23SIReadResponse{});
    }

    auto response = dto::K23SIReadResponse();
    response.value = rec->value.share();
    return RPCResponse(dto::K23SIStatus::OK("read succeeded"), std::move(response));
}

// Helper for iterating over the indexer, modifies it to end() if iterator would go past the target schema
// or if it would go past begin() for reverse scan. Starting iterator must not be end() and must
// point to a record with the target schema
void K23SIPartitionModule::_scanAdvance(IndexerIterator& it, bool reverseDirection, const String& schema) {
    if (!reverseDirection) {
        ++it;
        if (it != _indexer.end() && it->first.schemaName != schema) {
            it = _indexer.end();
        }

        return;
    }

    if (it == _indexer.begin()) {
        it = _indexer.end();
    } else {
        --it;

        if (it->first.schemaName != schema) {
            it = _indexer.end();
        }
    }
}

// Helper for handleQuery. Returns an iterator to start the scan at, accounting for
// desired schema and (eventually) reverse direction scan
IndexerIterator K23SIPartitionModule::_initializeScan(const dto::Key& start, bool reverse, bool exclusiveKey) {
    auto key_it = _indexer.lower_bound(start);

    // For reverse direction scan, key_it may not be in range because of how lower_bound works, so fix that here.
    // IF start key is empty, it means this reverse scan start from end of table OR
    //      if lower_bound returns a _indexer.end(), it also means reverse scan should start from end of table;
    // ELSE IF lower_bound returns a key equal to start AND exclusiveKey is true, reverse advance key_it once;
    // ELSE IF lower_bound returns a key bigger than start, find the first key not bigger than start;
    if (reverse) {
        if (start.partitionKey == "" || key_it == _indexer.end()) {
            key_it = (++_indexer.rbegin()).base();
        } else if (key_it->first == start && exclusiveKey) {
            _scanAdvance(key_it, reverse, start.schemaName);
        } else if (key_it->first > start) {
            while (key_it->first > start) {
                _scanAdvance(key_it, reverse, start.schemaName);
            }
        }
    }

    if (key_it != _indexer.end() && key_it->first.schemaName != start.schemaName) {
        key_it = _indexer.end();
    }

    return key_it;
}

// Helper for handleQuery. Checks to see if the indexer scan should stop.
bool K23SIPartitionModule::_isScanDone(const IndexerIterator& it, const dto::K23SIQueryRequest& request,
                                       size_t response_size) {
    if (it == _indexer.end()) {
        return true;
    } else if (it->first == request.key) {
        // Start key as inclusive overrides end key as exclusive
        return false;
    } else if (!request.reverseDirection && it->first >= request.endKey &&
               request.endKey.partitionKey != "") {
        return true;
    } else if (request.reverseDirection && it->first <= request.endKey) {
        return true;
    } else if (request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) {
        return true;
    } else if (response_size == _config.paginationLimit()) {
        return true;
    }

    return false;
}

// Helper for handleQuery. Returns continuation token (aka response.nextToScan)
dto::Key K23SIPartitionModule::_getContinuationToken(const IndexerIterator& it,
                    const dto::K23SIQueryRequest& request, dto::K23SIQueryResponse& response, size_t response_size) {
    // Three cases where scan is for sure done:
    // 1. Record limit is reached
    // 2. Iterator is not end() but is >= user endKey
    // 3. Iterator is at end() and partition bounds contains endKey
    if ((request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) ||
        // Test for past user endKey:
        (it != _indexer.end() &&
            (request.reverseDirection ? it->first <= request.endKey : it->first >= request.endKey && request.endKey.partitionKey != "")) ||
        // Test for partition bounds contains endKey and we are at end()
        (it == _indexer.end() &&
            (request.reverseDirection ?
            _partition().startKey <= request.endKey.partitionKey :
            request.endKey.partitionKey <= _partition().endKey && request.endKey.partitionKey != ""))) {
        return dto::Key();
    }
    else if (it != _indexer.end()) {
        // This is the paginated case
        response.exclusiveToken = false;
        return it->first;
    }

    // This is the multi-partition case
    if (request.reverseDirection) {
        response.exclusiveToken = true;
        return dto::Key {
            request.key.schemaName,
            _partition().startKey,
            ""
        };
    } else {
        response.exclusiveToken = false;
        return dto::Key {
            request.key.schemaName,
            _partition().endKey,
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
    Status status = dto::K23SIStatus::OK("");

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
K23SIPartitionModule::handleQuery(dto::K23SIQueryRequest&& request, dto::K23SIQueryResponse&& response, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "Partition: {}, received query {}", _partition, request);

    Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIQueryResponse{});
    }
    if (_partition.getHashScheme() != dto::HashScheme::Range) {
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("Query not implemented for hash partitioned collection"), dto::K23SIQueryResponse{});
    }

    IndexerIterator key_it = _initializeScan(request.key, request.reverseDirection, request.exclusiveKey);
    for (; !_isScanDone(key_it, request, response.results.size());
                        _scanAdvance(key_it, request.reverseDirection, request.key.schemaName)) {
        auto& versions = key_it->second;
        DataRecord* record = _getDataRecord(versions, request.mtr.timestamp);
        bool needPush = !record ? _needPush(versions, request.mtr.timestamp) : false;

        if (!record && !needPush) {
            // happy case: we either had no versions, or all versions were newer than the requested timestamp
            continue;
        }

        // happy case: either committed, or txn is reading its own write
        if (record) {
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
        K2LOG_D(log::skvsvr, "Partition {}, query from txn {}, updates read cache for key range {} - {}",
                _partition, request.mtr, request.key, key_it->first);

        // Do a push but we need to save our place in the query
        // TODO we can test the filter condition against the WI and last committed version and possibly
        // avoid a push
        // Must update read cache before doing an async operation
        request.reverseDirection ?
            _readCache->insertInterval(key_it->first, request.key, request.mtr.timestamp) :
            _readCache->insertInterval(request.key, key_it->first, request.mtr.timestamp);

        K2LOG_D(log::skvsvr, "About to PUSH in query request");
        request.key = key_it->first; // if we retry, do so with the key we're currently iterating on
        return _doPush(request.collectionName, key_it->first, versions.WI.txnId, request.mtr, deadline)
        .then([this, request=std::move(request),
                        resp=std::move(response), deadline](bool retryChallenger) mutable {
            if (!retryChallenger) {
                // sitting transaction won. Abort the incoming request
                return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in query push"), dto::K23SIQueryResponse{});
            }
            return handleQuery(std::move(request), std::move(resp), deadline);
        });
    }

    // Read cache update block
    dto::Key endInterval;
    if (key_it == _indexer.end()) {
        // For forward direction we need to lock the whole range of the schema, which we do
        // by appending a character, which may overshoot the range but is correct
        endInterval.schemaName = request.reverseDirection ? request.key.schemaName : request.key.schemaName + "a";
        endInterval.partitionKey = "";
        endInterval.rangeKey = "";
    } else {
        endInterval = key_it->first;
    }

    K2LOG_D(log::skvsvr, "Partition {}, query from txn {}, updates read cache for key range {} - {}",
                _partition, request.mtr, request.key, endInterval);
    request.reverseDirection ?
        _readCache->insertInterval(endInterval, request.key, request.mtr.timestamp) :
        _readCache->insertInterval(request.key, endInterval, request.mtr.timestamp);


    response.nextToScan = _getContinuationToken(key_it, request, response, response.results.size());
    K2LOG_D(log::skvsvr, "nextToScan: {}, exclusiveToken: {}", response.nextToScan, response.exclusiveToken);
    return RPCResponse(dto::K23SIStatus::OK("Query success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "Partition: {}, received read {}", _partition, request);

    Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIReadResponse{});
    }

    K2LOG_D(log::skvsvr, "Partition {}, read from txn {}, updates read cache for key {}",
                _partition, request.mtr, request.key);
    // update the read cache to lock out any future writers which may attempt to modify the key range
    // before this read's timestamp
    _readCache->insertInterval(request.key, request.key, request.mtr.timestamp);

    // find the record we should return
    auto* rec = _getDataRecord(request.key, request.mtr.timestamp);
    if (!rec) {
        return _makeReadOK(nullptr);
    }

    // happy case: either committed, or txn is reading its own write
    if (rec->status == dto::DataRecord::Committed || rec->txnId.mtr == request.mtr) {
        return _makeReadOK(rec);
    }
    // record is still pending and isn't from same transaction.
    return _doPush(request.collectionName, request.key, rec->txnId, request.mtr, deadline)
        .then([this, request=std::move(request), deadline](bool retryChallenger) mutable {
            if (!retryChallenger) {
                return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in read push"), dto::K23SIReadResponse{});
            }
            return handleRead(std::move(request), deadline);
        });
}

template <typename RequestT>
Status K23SIPartitionModule::_validateStaleWrite(const RequestT& request, VersionsT& versions) {
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        return dto::K23SIStatus::AbortRequestTooOld("write request is outside retention window");
    }
    // check read cache for R->W conflicts
    auto ts = _readCache->checkInterval(request.key, request.key);
    if (request.mtr.timestamp.compareCertain(ts) < 0) {
        // this key range was read more recently than this write
        K2LOG_D(log::skvsvr, "Partition: {}, read cache validation failed for key: {}, transaction timestamp: {}, < readCache key timestamp: {}, readcache min_TimeStamp: {}", _partition, request.key, request.mtr.timestamp, ts, _readCache->min_TimeStamp());
        bool belowReadCacheWaterMark = (request.mtr.timestamp.compareCertain(_readCache->min_TimeStamp()) <= 0);
        if (belowReadCacheWaterMark)
        {
            return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as this key (or key range) is older than min timestamp(watermark) server maintains.");
        }
        else
        {
            return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as this key (or key range) has been observed by another transaction");
        }
    }

    // check if we have a committed value newer than the request. The latest committed
    // is either the first or second in deque as we may have at most one outstanding WI
    // NB(1) if we try to place a WI over a committed value from different transaction with same ts.end
    // (even if from different TSO), reject the incoming write in order to avoid weird read-my-write problem
    // for in-progress transactions
    // NB(2) we cannot allow writes past a committed value since a write has to imply a read causality, so
    // if a txn committed a value at time T5, then we must also assume they did a read at time T5
    // NB(3) if we encounter a WI, we check the second oldest version to see if there is a need to push.
    // If the second oldest is newer than we are, then we won't commit even if we win a PUSH against the WI.
    if (versions.size() > 0 && versions[0].status == dto::DataRecord::Committed &&
        request.mtr.timestamp.compareCertain(versions[0].txnId.mtr.timestamp) <= 0) {
        // newest version is the latest committed and its newer than the request
        // or committed version from same transaction is found (e.g. bad retry on a write came through after commit)
        K2LOG_D(log::skvsvr, "Partition: {}, failing write older than latest commit for key {}", _partition, request.key);
        return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as we have a newer committed write for this key from another transaction");
    }
    else if (versions.size() > 1 && versions[0].status == dto::DataRecord::WriteIntent &&
        request.mtr.timestamp.compareCertain(versions[1].txnId.mtr.timestamp) <= 0) {
        // second newest version is the latest committed and its newer than the request.
        // no need to push since this request would fail anyway against the committed value
        K2LOG_D(log::skvsvr, "Partition: {}, failing write older than latest commit for key {}", _partition, request.key);
        return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as we have a newer committed write (and wi) for this key from other transactions");
    }

    K2LOG_D(log::skvsvr, "Partition: {}, stale write check passed for key {}", _partition, request.key);
    return dto::K23SIStatus::OK("");
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
    T value{};
    success = payload.read(value);
}

template <typename T>
void _copyPayloadBaseToUpdate(const dto::SchemaField& field, Payload& base, Payload& update, bool& success) {
    (void) field;
    T value{};
    success = base.read(value);
    if (!success) {
        return;
    }

    update.write(value);
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
    Payload basePayload = version.value.fieldData.shareAll();   // base payload
    Payload payload(Payload::DefaultAllocator);                     // payload for new record

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
    std::size_t findField; // find field index of base SKVRecord
    std::vector<uint32_t> fieldsOffset(1); // every fields offset of base SKVRecord
    std::size_t baseCursor = 0; // indicate fieldsOffset cursor

    Payload basePayload = version.value.fieldData.shareAll();   // base payload
    Payload payload(Payload::DefaultAllocator);                     // payload for new record

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
    Payload projectedPayload(Payload::DefaultAllocator);            // payload for projection

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


seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::handleWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline) {
    // NB: failures in processing a write do not require that we set the TR state to aborted at the TRH. We rely on
    //     the client to do the correct thing and issue an abort on a failure.
    K2LOG_D(log::skvsvr, "Partition: {}, handle write: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2LOG_D(log::skvsvr, "Partition: {}, failed validation for {}", _partition, request.key);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in write"), dto::K23SIWriteResponse{});
    }
    if (!_validateRequestPartitionKey(request)){
        // do not allow empty partition key
        return RPCResponse(dto::K23SIStatus::BadParameter("missing partition key in write"), dto::K23SIWriteResponse{});
    }

    auto schemaIt = _schemas.find(request.key.schemaName);
    if (schemaIt == _schemas.end()) {
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("schema does not exist"), dto::K23SIWriteResponse{});
    }
    if (schemaIt->second.find(request.value.schemaVersion) == schemaIt->second.end()) {
        // server does not have schema
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("schema does not exist"), dto::K23SIWriteResponse{});
    }

    // at this point the request is valid. Check to see if we should be creating a TR
    // we want to create the TR now even if the write may fail due to some other constraints. In case
    // of such failure, the client is expected to come in and end the transaction with Abort
    if (request.designateTRH) {
        K2LOG_D(log::skvsvr, "Partition: {}, designating trh for key {}", _partition, request.key);
        return _txnMgr.onAction(TxnRecord::Action::onCreate, {.trh=request.trh, .mtr=request.mtr})
        .then([this, request=std::move(request), deadline]() mutable {
            K2LOG_D(log::skvsvr, "Partition: {}, tr created and re-driving request for key {}", _partition, request.key);
            request.designateTRH = false; // unset the flag and re-run
            return handleWrite(std::move(request), deadline);
        })
        .handle_exception_type([this](TxnManager::ClientError&) {
            // Failed to create
            K2LOG_D(log::skvsvr, "Partition: {}, failed creating TR", _partition);
            return RPCResponse(dto::K23SIStatus::AbortConflict("txn too old in write"), dto::K23SIWriteResponse{});
        });
    }

    auto& versions = _indexer[request.key];
    // in this situation, return AbortRequestTooOld error.
    {
        Status validateStatus = _validateStaleWrite(request, versions);
        if (!validateStatus.is2xxOK()) {
            K2LOG_D(log::skvsvr, "Partition: {}, request too old for key {} due to {}",
                    _partition, request.key, validateStatus);
            return RPCResponse(std::move(validateStatus), dto::K23SIWriteResponse{});
        }
    }

    // check to see if we should push or is this a write from same txn
    if (versions.size() > 0 && versions[0].status == dto::DataRecord::WriteIntent) {
        auto& rec = versions[0];
        auto& rqmtr = request.mtr;

        if (rec.txnId.mtr != rqmtr) {
            // this is a write request finding a WI from a different transaction. Do a push with the remaining
            // deadline time.
            K2LOG_D(log::skvsvr, "Partition: {}, different WI found for key {}", _partition, request.key);
            return _doPush(request.collectionName, rec.key, rec.txnId, request.mtr, deadline)
                .then([this, request = std::move(request), deadline](auto&& retryChallenger) mutable {
                    if (retryChallenger) {
                        K2LOG_D(log::skvsvr, "Partition: {}, write push retry for key {}", _partition, request.key);
                        return handleWrite(std::move(request), deadline);
                    }
                    // challenger must fail
                    K2LOG_D(log::skvsvr, "Partition: {}, write push challenger lost for key {}", _partition, request.key);
                    return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in write push"), dto::K23SIWriteResponse{});
                });
        }
    }

    if (request.rejectIfExists && versions.size() > 0 && !versions[0].isTombstone) {
        // Need to add to read cache to prevent an erase coming in before this requests timestamp
        // If the condition passes (ie, there was no previous version and the insert succeeds) then
        // we do not need to insert into the read cache because the write intent will handle conflicts
        // and if the transaction aborts then any state it implicitly observes does not matter
        K2LOG_D(log::skvsvr, "Partition {}, write from txn {}, updates read cache for key {}", _partition, request.mtr, request.key);
        _readCache->insertInterval(request.key, request.key, request.mtr.timestamp);

        // The ConditionFailed status does not mean that the transaction must abort. It is up to the user
        // to decide to abort or not, similar to a KeyNotFound status on read.
        return RPCResponse(dto::K23SIStatus::ConditionFailed("Previous record exists"), dto::K23SIWriteResponse{});
    }

    if (request.fieldsForPartialUpdate.size() > 0) {
        // parse the partial record to full record
        if ( !versions.size() || versions[0].isTombstone) {
            // cannot parse partial record without a version
            return RPCResponse(dto::K23SIStatus::KeyNotFound("can not partial update with no/deleted version"), dto::K23SIWriteResponse{});
        }
        if (!_parsePartialRecord(request, versions[0])) {
            K2LOG_D(log::skvsvr, "Partition: {}, can not parse partial record for key {}", _partition, request.key);
            versions[0].value.fieldData.seek(0);
            return RPCResponse(dto::K23SIStatus::BadParameter("missing fields or can not interpret partialUpdate"), dto::K23SIWriteResponse{});
        }
    }

    // Clean up if req and WI are in the same transaction
    if (versions.size() > 0 && versions[0].txnId.mtr == request.mtr) {
        versions.pop_front();
    }

    // all checks passed - we're ready to place this WI as the latest version(at head of versions deque)
    return _createWI(std::move(request), versions, deadline).then([this]() mutable {
        K2LOG_D(log::skvsvr, "Partition: {}, WI created", _partition);
        return RPCResponse(dto::K23SIStatus::Created("wi created"), dto::K23SIWriteResponse{});
    });
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
K23SIPartitionModule::handleTxnPush(dto::K23SITxnPushRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, push request: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in push"), dto::K23SITxnPushResponse());
    }
    if (!_validatePushRetention(request)){
        // the request is outside the retention window
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request(challenger MTR) too old in push"), dto::K23SITxnPushResponse());
    }
    dto::TxnId txnId{.trh=std::move(request.key), .mtr=std::move(request.incumbentMTR)};
    TxnRecord& incumbent = _txnMgr.getTxnRecord(txnId);

    switch(incumbent.state) {
        case dto::TxnRecordState::Created:
            // incumbent did not exist. Perform a force-abort.
            return _txnMgr
                .onAction(TxnRecord::Action::onForceAbort, std::move(txnId))
                .then([mtr=std::move(request.challengerMTR)] {
                    return RPCResponse(dto::K23SIStatus::OK("challenger won in push"),
                        dto::K23SITxnPushResponse{.winnerMTR = std::move(mtr),
                                                .incumbentState=dto::TxnRecordState::Aborted, // incumbent is now aborted
                                                .allowChallengerRetry=true} // let the challenger retry
                    );
                });
        case dto::TxnRecordState::InProgress: {
            // must pick a victim
            bool abortIncumbent = false;
            // #1 abort based on priority
            if (incumbent.txnId.mtr.priority > request.challengerMTR.priority) {  // bigger number means lower priority
                K2LOG_D(log::skvsvr, "Partition: {}, aborting incumbent for key {}", _partition, txnId.trh);
                abortIncumbent = true;
            }
            // #2 if equal, pick the newer transaction
            else if (incumbent.txnId.mtr.priority == request.challengerMTR.priority) {
                auto cmpResult = incumbent.txnId.mtr.timestamp.compareCertain(request.challengerMTR.timestamp);
                if (cmpResult == dto::Timestamp::LT) {
                    K2LOG_D(log::skvsvr, "Partition: {}, aborting incumbent for key {}", _partition, txnId.trh);
                    abortIncumbent = true;
                } else if (cmpResult == dto::Timestamp::EQ) {
                    // #3 if same priority and timestamp, abort on tso ID which must be unique
                    if (incumbent.txnId.mtr.timestamp.tsoId() < request.challengerMTR.timestamp.tsoId()) {
                        K2LOG_D(log::skvsvr, "Partition: {}, aborting incumbent for key {}", _partition, txnId.trh);
                        abortIncumbent = true;
                    } else {
                        // make sure we don't have a bug - the timestamps cannot be the same
                        K2ASSERT(log::skvsvr, incumbent.txnId.mtr.timestamp.tsoId() != request.challengerMTR.timestamp.tsoId(), "invalid timestamps detected");
                    }
                }
            }
            // #3 abort the challenger
            else {
                // this branch isn't needed as it is the fall-through option, but keeping it here for clarity
                K2LOG_D(log::skvsvr, "Partition: {}, aborting challenger for key {}", _partition, txnId.trh);
                abortIncumbent = false;
            }

            if (abortIncumbent) {
                return _txnMgr
                    .onAction(TxnRecord::Action::onForceAbort, std::move(txnId))
                    .then([mtr=std::move(request.challengerMTR)] {
                        return RPCResponse(dto::K23SIStatus::OK("challenger won in push"),
                            dto::K23SITxnPushResponse{.winnerMTR = std::move(mtr),
                                                    .incumbentState=dto::TxnRecordState::Aborted, // incumbent is now aborted
                                                    .allowChallengerRetry=true} // let the challenger retry
                        );
                    });
            }
            else {
                return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                                   dto::K23SITxnPushResponse{.winnerMTR = std::move(txnId.mtr),
                                                             .incumbentState = dto::TxnRecordState::InProgress,
                                                             .allowChallengerRetry=false});
            }
            break;
        }
        case dto::TxnRecordState::ForceAborted:
            // fall-through
        case dto::TxnRecordState::Aborted:
            // let client know that incumbent has been aborted and they can retry
            return RPCResponse(dto::K23SIStatus::OK("challenger won in push since incumbent was already aborted"),
                dto::K23SITxnPushResponse{.winnerMTR = std::move(request.challengerMTR),
                                          .incumbentState=dto::TxnRecordState::Aborted, // incumbent is now aborted
                                          .allowChallengerRetry=true} // let the challenger retry
            );
        case dto::TxnRecordState::Committed:
            return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                dto::K23SITxnPushResponse{.winnerMTR = std::move(txnId.mtr),
                                        .incumbentState=dto::TxnRecordState::Committed,
                                        .allowChallengerRetry=true}
            );
        case dto::TxnRecordState::Deleted:
            // possible race condition - the incumbent has just finished finalizing and
            // is being removed from memory. The caller should not see this as a WI anymore
            return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"),
                dto::K23SITxnPushResponse{.winnerMTR = std::move(txnId.mtr),
                                        .incumbentState=dto::TxnRecordState::Deleted,
                                        .allowChallengerRetry=true}
            );
        default:
            K2ASSERT(log::skvsvr, false, "Invalid transaction state: {}", incumbent.state);
    }
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
K23SIPartitionModule::handleTxnEnd(dto::K23SITxnEndRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, transaction end: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2LOG_D(log::skvsvr, "Partition: {}, transaction end too old for txn={}", _partition, request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in end"), dto::K23SITxnEndResponse());
    }

    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        K2LOG_D(log::skvsvr, "Partition: {}, transaction end outside retention for txn={}", _partition, request.mtr);
        return _txnMgr.onAction(TxnRecord::Action::onRetentionWindowExpire,
                            {.trh=std::move(request.key), .mtr=std::move(request.mtr)})
                .then([]() {
                    return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request too old in end"), dto::K23SITxnEndResponse());
                });
    }
    dto::TxnId txnId{.trh = std::move(request.key), .mtr = std::move(request.mtr)};

    // this action always needs to be executed against the transaction to see what would happen.
    // If we can successfully execute the action, then it's a success response. Otherwise, the user
    // receives an error response which is telling them that the transaction has been aborted
    auto action = request.action == dto::EndAction::Commit ? TxnRecord::Action::onEndCommit : TxnRecord::Action::onEndAbort;

    // store the write keys into the txnrecord
    TxnRecord& rec = _txnMgr.getTxnRecord(txnId);
    rec.writeKeys = std::move(request.writeKeys);
    rec.syncFinalize = request.syncFinalize;
    rec.timeToFinalize = request.timeToFinalize;

    // and just execute the transition
    return _txnMgr.onAction(action, std::move(txnId))
        .then([this] {
            // action was successful
            K2LOG_D(log::skvsvr, "Partition: {}, transaction ended", _partition);
            return RPCResponse(dto::K23SIStatus::OK("transaction ended"), dto::K23SITxnEndResponse());
        })
        .handle_exception_type([this](TxnManager::ClientError&) {
            K2LOG_D(log::skvsvr, "Partition: {}, failed transaction end", _partition);
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("transaction state transition not allowed in end"), dto::K23SITxnEndResponse());
        });
}

seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
K23SIPartitionModule::handleTxnHeartbeat(dto::K23SITxnHeartbeatRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, transaction hb: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2LOG_D(log::skvsvr, "Partition: {}, txn hb too old txn={}", _partition, request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in hb"), dto::K23SITxnHeartbeatResponse());
    }
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        K2LOG_D(log::skvsvr, "Partition: {}, txn hb too old txn={}", _partition, request.mtr);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("txn too old in hb"), dto::K23SITxnHeartbeatResponse());
    }

    return _txnMgr.onAction(TxnRecord::Action::onHeartbeat, dto::TxnId{.trh=std::move(request.key), .mtr=std::move(request.mtr)})
    .then([this]() {
        // heartbeat was applied successfully
        K2LOG_D(log::skvsvr, "Partition: {}, txn hb success", _partition);
        return RPCResponse(dto::K23SIStatus::OK("hb succeeded"), dto::K23SITxnHeartbeatResponse());
    })
    .handle_exception_type([this] (TxnManager::ClientError&) {
        // there was a problem applying the heartbeat due to client's view of the TR state. Client should abort
        K2LOG_D(log::skvsvr, "Partition: {}, txn hb fail", _partition);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("hb not allowed for the txn state"), dto::K23SITxnHeartbeatResponse{});
    });
}

seastar::future<bool>
K23SIPartitionModule::_doPush(String collectionName, dto::Key key, dto::TxnId incumbentTxnId, dto::K23SI_MTR challengerMTR, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "partition: {}, executing push against txnid={}, for mtr={}", _partition, incumbentTxnId, challengerMTR);
    dto::K23SITxnPushRequest request{};
    request.collectionName = std::move(collectionName);
    request.incumbentMTR = std::move(incumbentTxnId.mtr);
    request.key = std::move(incumbentTxnId.trh); // this is the routing key - should be the TRH key
    request.challengerMTR = std::move(challengerMTR);
    return seastar::do_with(std::move(request), std::move(key), [this, deadline] (auto& request, auto& key) {
        return _cpo.PartitionRequest<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse, dto::Verbs::K23SI_TXN_PUSH>(deadline, request)
        .then([this, &key, &request](auto&& responsePair) {
            auto& [status, response] = responsePair;
            K2LOG_D(log::skvsvr, "Push request completed with status={} and response={}", status, response);
            if (status != dto::K23SIStatus::OK) {
                K2LOG_E(log::skvsvr, "Partition: {}, txn push failed: {}", _partition, status);
                return seastar::make_exception_future<bool>(TxnManager::ServerError());
            }

            // update the write intent if necessary
            dto::WriteIntent* rec = _getWriteIntent(key, request.incumbentMTR.timestamp);
            if (rec) {
                switch (response.incumbentState) {
                    case dto::TxnRecordState::InProgress: {
                        break;
                    }
                    case dto::TxnRecordState::Aborted: {
                        rec->status = dto::DataRecord::Aborted;
                        //NB this call invalidates rec since we're modifying the indexer
                        _removeRecord(key, request.incumbentMTR.timestamp); // TODO-persistence: This shouldn't be done here but after successful persist, probably during txn finalization and/or GC for abandoned WIs
                        break;
                    }
                    case dto::TxnRecordState::Committed: {
                        // TODO-persistence this needs to be persisted
                        _commitWriteIntent(key, , request.incumbentMTR.timestamp);
                        break;
                    }
                    case dto::TxnRecordState::Deleted: {
                        K2LOG_E(log::skvsvr, "Invalid write intent. Transaction is in state Deleted but WI is still present and not finalized in txn {}", rec->txnId);
                        break;
                    }
                    default:
                        K2LOG_E(log::skvsvr, "Unable to convert WI state based on txn state: {}, in txn: {}", response.incumbentState, rec->txnId);
                }
            }

            // signal the caller what to do with the challenger
            return seastar::make_ready_future<bool>(response.allowChallengerRetry);
        });
    });
}

seastar::future<>
K23SIPartitionModule::_createWI(dto::K23SIWriteRequest&& request, VersionSet& versions, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "Partition: {}, Write Request creating WI: {}", _partition, request);
    dto::DataRecord rec;
    // we need to copy this data into a new memory block so that we don't hold onto and fragment the transport memory
    rec.value = request.value.copy();
    rec.isTombstone = request.isDelete;
    TxnId txnId = dto::TxnId{.trh = std::move(request.trh), .mtr = std::move(request.mtr)};

    versions.WI.emplace(std::move(rec), std::move(txnId), dto::DataRecord::WriteIntent, request.request_id);

    // TODO write to WAL
    return _persistence.makeCall(versions.WI, deadline);
}

seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
K23SIPartitionModule::handleTxnFinalize(dto::K23SITxnFinalizeRequest&& request) {
    // find the version deque for the key
    K2LOG_D(log::skvsvr, "Partition: {}, txn finalize: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in finalize"), dto::K23SITxnFinalizeResponse());
    }
    if (!_validateRequestPartitionKey(request)){
        // do not allow empty partition key
        return RPCResponse(dto::K23SIStatus::BadParameter("missing partition key in finalize"), dto::K23SITxnFinalizeResponse());
    }

    // get the data record for this key
    // TODO-persistence We should handle the cases when the record is updated in-memory but not persisted yet
    auto* rec = _getDataRecord(request.key, request.mtr.timestamp);

    dto::TxnId txnId{.trh=std::move(request.trh), .mtr=std::move(request.mtr)};
    if (!rec || rec->txnId != txnId || rec->txnId.trh != txnId.trh) {
        // we don't have a record from this transaction
        if (request.action == dto::EndAction::Abort) {
            // we don't have it but it was an abort anyway
            K2LOG_D(log::skvsvr, "Partition: {}, abort for missing version {}, in txn {}", _partition, request.key, txnId);
            return RPCResponse(dto::K23SIStatus::OK("finalize key missing in abort"), dto::K23SITxnFinalizeResponse());
        }
        // we can't allow the commit since we don't have the write intent and we don't have a committed version
        K2LOG_D(log::skvsvr, "Partition: {}, rejecting commit for missing version {}, in txn {}", _partition, request.key, txnId);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("cannot commit missing key"), dto::K23SITxnFinalizeResponse());
    }

    // we found a record from this transaction
    switch(rec->status) {
        case dto::DataRecord::WriteIntent: {
            // if it is currently a write intent, modify as needed
            if (request.action == dto::EndAction::Commit) {
                K2LOG_D(log::skvsvr, "Partition: {}, committing {}, in txn {}", _partition, request.key, txnId);
                rec->status = dto::DataRecord::Committed;
            }
            else {
                K2LOG_D(log::skvsvr, "Partition: {}, aborting {}, in txn {}", _partition, request.key, txnId);
                rec->status = dto::DataRecord::Aborted;
            }
            break;
        }
        case dto::DataRecord::Committed:
            // don't trigger the failure response if the action matches the state
            if (request.action == dto::EndAction::Commit) break;
            K2LOG_D(log::skvsvr, "Partition: {}, cannot abort committed record: {}", _partition, *rec);
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("cannot finalize txn"), dto::K23SITxnFinalizeResponse());
        case dto::DataRecord::Aborted:
            // don't trigger the failure response if the action matches the state
            if (request.action == dto::EndAction::Abort) break;
            K2LOG_D(log::skvsvr, "Partition: {}, cannot commit aborted record: {}", _partition, *rec);
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("cannot finalize txn"), dto::K23SITxnFinalizeResponse());
        default:
            // the action did not match the state
            K2LOG_D(log::skvsvr,
                "Partition: {}, failing finalize due to action mismatch {}, in txn {}, have status={}, asked={}",
                _partition, request.key, txnId, rec->status, request.action);
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("cannot finalize txn"), dto::K23SITxnFinalizeResponse());
    }

    // TODO-persistence: For now, remove aborted records right-away. With persistence we should do so after successfully
    // persisting
    if (rec->status == dto::DataRecord::Aborted) {
        _removeRecord(*rec); // NB: rec is now invalid since we're modifying the indexer
    }

    // send a partial update for updating the status of the record
    return _persistence.makeCall(dto::K23SI_PersistencePartialUpdate{}, _config.persistenceTimeout()).then([] {
        return RPCResponse(dto::K23SIStatus::OK("persistence call succeeded"), dto::K23SITxnFinalizeResponse{});
    });
}

seastar::future<std::tuple<Status, dto::K23SIPushSchemaResponse>>
K23SIPartitionModule::handlePushSchema(dto::K23SIPushSchemaRequest&& request) {
    K2LOG_D(log::skvsvr, "handlePushSchema for schema: {}", request.schema.name);
    if (_cmeta.name != request.collectionName) {
        return RPCResponse(Statuses::S403_Forbidden("Collection names in partition and request do not match"), dto::K23SIPushSchemaResponse{});
    }

    _schemas[request.schema.name][request.schema.version] = std::make_shared<dto::Schema>(request.schema);

    return RPCResponse(Statuses::S200_OK("push schema success"), dto::K23SIPushSchemaResponse{});
}

// For test and debug purposes, not normal transaction processsing
// Returns all versions+WIs for a particular key
seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
K23SIPartitionModule::handleInspectRecords(dto::K23SIInspectRecordsRequest&& request) {
    K2LOG_D(log::skvsvr, "handleInspectRecords for: {}", request.key);

    auto it = _indexer.find(request.key);
    if (it == _indexer.end()) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound("Key not found in indexer"), dto::K23SIInspectRecordsResponse{});
    }
    auto& versions = it->second;

    std::vector<dto::DataRecord> records;
    records.reserve(versions.size());

    for (dto::DataRecord& rec : versions) {
        dto::DataRecord copy {
            rec.key,
            rec.value.share(),
            rec.isTombstone,
            rec.txnId,
            rec.status
        };

        records.push_back(std::move(copy));
    }

    dto::K23SIInspectRecordsResponse response {
        std::move(records)
    };
    return RPCResponse(dto::K23SIStatus::OK("Inspect records success"), std::move(response));
}

// For test and debug purposes, not normal transaction processsing
// Returns the specified TRH
seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
K23SIPartitionModule::handleInspectTxn(dto::K23SIInspectTxnRequest&& request) {
    K2LOG_D(log::skvsvr, "handleInspectTxn key={}, mtr={}", request.key, request.mtr);

    dto::TxnId id{std::move(request.key), std::move(request.mtr)};
    TxnRecord* txn = _txnMgr.getTxnRecordNoCreate(id);
    if (!txn) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound("TRH not found"), dto::K23SIInspectTxnResponse{});
    }

    K23SIInspectTxnResponse response {
        txn->txnId,
        txn->writeKeys,
        txn->rwExpiry,
        txn->syncFinalize,
        txn->state
    };
    return RPCResponse(dto::K23SIStatus::OK("Inspect txn success"), std::move(response));
}

// For test and debug purposes, not normal transaction processsing
// Returns all WIs on this node for all keys
seastar::future<std::tuple<Status, dto::K23SIInspectWIsResponse>>
K23SIPartitionModule::handleInspectWIs(dto::K23SIInspectWIsRequest&& request) {
    (void) request;
    K2LOG_D(log::skvsvr, "handleInspectWIs");
    std::vector<dto::DataRecord> records;

    for (auto it = _indexer.begin(); it != _indexer.end(); ++it) {
        auto& versions = it->second;
        for (dto::DataRecord& rec : versions) {
            if (rec.status != dto::DataRecord::Status::WriteIntent) {
                continue;
            }

            dto::DataRecord copy {
                rec.key,
                rec.value.share(),
                rec.isTombstone,
                rec.txnId,
                rec.status
            };

            records.push_back(std::move(copy));
        }
    }

    dto::K23SIInspectWIsResponse response { std::move(records) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect WIs success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIInspectAllTxnsResponse>>
K23SIPartitionModule::handleInspectAllTxns(dto::K23SIInspectAllTxnsRequest&& request) {
    (void) request;
    K2LOG_D(log::skvsvr, "handleInspectAllTxns");

    std::vector<dto::K23SIInspectTxnResponse> txns;
    txns.reserve(_txnMgr._transactions.size());

    for (auto it = _txnMgr._transactions.begin(); it != _txnMgr._transactions.end(); ++it) {
        K23SIInspectTxnResponse copy {
            it->second.txnId,
            it->second.writeKeys,
            it->second.rwExpiry,
            it->second.syncFinalize,
            it->second.state
        };

        txns.push_back(std::move(copy));
    }

    dto::K23SIInspectAllTxnsResponse response { std::move(txns) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect all txns success"), std::move(response));
}

// For test and debug purposes, not normal transaction processsing
// Returns all keys on this node
seastar::future<std::tuple<Status, dto::K23SIInspectAllKeysResponse>>
K23SIPartitionModule::handleInspectAllKeys(dto::K23SIInspectAllKeysRequest&& request) {
    (void) request;
    K2LOG_D(log::skvsvr, "handleInspectAllKeys");
    std::vector<dto::Key> keys;
    keys.reserve(_indexer.size());

    for (auto it = _indexer.begin(); it != _indexer.end(); ++it) {
        keys.push_back(it->first);
    }

    dto::K23SIInspectAllKeysResponse response { std::move(keys) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect AllKeys success"), std::move(response));
}

// get the data record with the given key which is not newer than the given timestsamp, or if it
// is an exact match for a write intent (for read your own writes, etc.)
dto::DataRecord*
K23SIPartitionModule::_getDataRecord(const dto::Key& key, const dto::Timestamp& timestamp) {
    auto versions = _indexer.find(key);
    if (versions == _indexer.end()) {
        return nullptr;
    }
    auto viter = _getVersion(versions->second, timestamp);
    if (viter == versions->second.end()) {
        return nullptr;
    }
    return &(*viter);
}

void K23SIPartitionModule::_removeRecord(dto::DataRecord& rec) {
    auto kiter = _indexer.find(rec.key);
    if (kiter != _indexer.end() && !kiter->second.empty()) {
        auto viter = _getVersion(kiter->second, rec.txnId.mtr.timestamp);
        if (viter != kiter->second.end()) {
            K2LOG_D(log::skvsvr, "Partition: {}, removing aborted version for key={}, from txn={}", _partition, rec.key, rec.txnId);
            K2ASSERT(log::skvsvr, viter->status == dto::DataRecord::Aborted, "Record not in Aborted state: {}", (*viter));
            kiter->second.erase(viter);
            if (kiter->second.empty()) {
                _indexer.erase(kiter);
            }
        }
    }
}

} // ns k2
