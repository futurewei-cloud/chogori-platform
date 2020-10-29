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
#include <k2/dto/MessageVerbs.h>
#include <k2/appbase/AppEssentials.h>
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
        K2DEBUG("Partition: " << _partition << ", refreshing retention timestamp");
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
    K2INFO("ctor for cname=" << _cmeta.name <<", part=" << _partition);
}

seastar::future<> K23SIPartitionModule::start() {
    K2DEBUG("Starting for partition: " << _partition);
    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse>
    (dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
        return handleRead(std::move(request), dto::K23SI_MTR_ZERO, FastDeadline(_config.readTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SIQueryRequest, dto::K23SIQueryResponse>
    (dto::Verbs::K23SI_QUERY, [this](dto::K23SIQueryRequest&& request) {
        return handleQuery(std::move(request), dto::K23SIQueryResponse{}, FastDeadline(_config.readTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest, dto::K23SIWriteResponse>
    (dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest&& request) {
        return handleWrite(std::move(request), dto::K23SI_MTR_ZERO, FastDeadline(_config.writeTimeout()));
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


    if (_cmeta.retentionPeriod < _config.minimumRetentionPeriod()) {
        K2WARN("Requested retention(" << _cmeta.retentionPeriod << ") is lower than minimum("
                                      << _config.minimumRetentionPeriod() << "). Extending retention to minimum");
        _cmeta.retentionPeriod = _config.minimumRetentionPeriod();
    }

    // todo call TSO to get a timestamp
    return getTimeNow()
        .then([this](dto::Timestamp&& watermark) {
            K2DEBUG("Cache watermark: " << watermark << ", period=" << _cmeta.retentionPeriod);
            _retentionTimestamp = watermark - _cmeta.retentionPeriod;
            _readCache = std::make_unique<ReadCache<dto::Key, dto::Timestamp>>(watermark, _config.readCacheSize());
            _retentionUpdateTimer.arm(_config.retentionTimestampUpdateInterval());
            return seastar::when_all_succeed(_recovery(), _txnMgr.start(_cmeta.name, _retentionTimestamp, _cmeta.heartbeatDeadline)).discard_result();
        });
}

K23SIPartitionModule::~K23SIPartitionModule() {
    K2INFO("dtor for cname=" << _cmeta.name <<", part=" << _partition);
}

seastar::future<> K23SIPartitionModule::_recovery() {
    //TODO perform recovery
    K2DEBUG("Partition: " << _partition << ", recovery");
    return _persistence.makeCall(dto::K23SI_PersistenceRecoveryRequest{}, _config.persistenceTimeout());
}

seastar::future<> K23SIPartitionModule::gracefulStop() {
    K2INFO("stop for cname=" << _cmeta.name << ", part=" << _partition);
    _retentionUpdateTimer.cancel();
    return seastar::when_all_succeed(std::move(_retentionRefresh), _txnMgr.gracefulStop()).discard_result().then([]{K2INFO("stopped");});
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
void K23SIPartitionModule::_scanAdvance(IndexerIterator& it, bool reverseDirection) {
    const String& schema = it->first.schemaName;

    if (!reverseDirection) {
        ++it;
        if (it != _indexer.end() && it->first.schemaName != schema) {
            std::cout << "{_scanAdvance} 1" << std::endl;
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
        if (start == "" || key_it == _indexer.end()) {
            key_it = _indexer.rbegin();
        } else if (key_it->first == start && exclusiveKey) {
            _scanAdvance(key_it, reverse);
        } else if (key_it->first > start) {
            while (key_it->first > start) {
                _scanAdvance(key_it, reverse);
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
        std::cout << "{_isScanDone} 1" << std::endl;
        return true;
    } else if (!request.reverseDirection && it->first >= request.endKey &&
               request.endKey.partitionKey != "") {
        std::cout << "{_isScanDone} 2" << std::endl;
        return true;
    } else if (request.reverseDirection && it->first <= request.endKey) {
        std::cout << "{_isScanDone} 3" << std::endl;
        return true;
    } else if (request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) {
        std::cout << "{_isScanDone} 4" << std::endl;
        return true;
    } else if (response_size == _config.paginationLimit()) {
        std::cout << "{_isScanDone} 5" << std::endl;
        return true;
    }

    return false;
}

// Helper for handleQuery. Returns continuation token (aka response.nextToScan)
dto::Key K23SIPartitionModule::_getContinuationToken(const IndexerIterator& it,
                    const dto::K23SIQueryRequest& request, size_t response_size) {
    // Three cases where scan is for sure done:
    // 1. Record limit is reached
    // 2. Iterator is not end() but is >= user endKey
    // 3. Iterator is at end() and partition bounds contains endKey
    // This also works around seastars lack of operators on the string type
    if ((request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) ||
        // Test for past user endKey:
        (it != _indexer.end() &&
            (request.reverseDirection ? it->first <= request.endKey : it->first >= request.endKey && request.endKey.partitionKey != "")) ||
        // Test for partition bounds contains endKey and we are at end()
        (it == _indexer.end() &&
            (request.reverseDirection ?
            _partition().startKey < request.endKey.partitionKey :
            request.endKey.partitionKey < _partition().endKey && request.endKey.partitionKey != "")) ||
        (it == _indexer.end() &&
            (request.reverseDirection ?
            request.endKey.partitionKey == _partition().startKey :
            request.endKey.partitionKey == _partition().endKey))) {
        return dto::Key();
    }
    else if (it != _indexer.end()) {
        // This is the paginated case
        request.exclusiveKey = false;
        return it->first;
    }

    // This is the multi-partition case
    if (request.reverseDirection) {
        request.exclusiveKey = true;
        return dto::Key {
            request.key.schemaName,
            _partition().startKey,
            ""
        };
    } else {
        request.exclusiveKey = false;
        return dto::Key {
            request.key.schemaName,
            _partition().endKey,
            ""
        };
    }
}

seastar::future<std::tuple<Status, dto::K23SIQueryResponse>>
K23SIPartitionModule::handleQuery(dto::K23SIQueryRequest&& request, dto::K23SIQueryResponse&& response, FastDeadline deadline) {
    K2DEBUG("Partition: " << _partition << ", received query " << request);
    std::cout << "{request} key:" << request.key << ", endKey:" << request.endKey << std::endl;
    std::cout << "{response size} " << response.results.size() << ", nextToScan:" << response.nextToScan << std::endl;

    Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIQueryResponse{});
    }
    if (_partition.getHashScheme() != dto::HashScheme::Range) {
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("Query not implemented for hash partitioned collection"), dto::K23SIQueryResponse{});
    }

    IndexerIterator key_it = _initializeScan(request.key, request.reverseDirection, request.exclusiveKey);

    for (; !_isScanDone(key_it, request, response.results.size());
                        _scanAdvance(key_it, request.reverseDirection)) {
        auto& versions = key_it->second;
        auto viter = versions.begin();
        // position the version iterator at the version we should be returning
        while(viter != versions.end() && request.mtr.timestamp.compareCertain(viter->txnId.mtr.timestamp) < 0) {
            ++viter;
        }
        if (viter == versions.end()) {
            // happy case: we either had no versions, or all versions were newer than the requested timestamp
            continue;
        }

        // happy case: either committed, or txn is reading its own write
        if (viter->status == dto::DataRecord::Committed || viter->txnId.mtr == request.mtr) {
            if (!viter->isTombstone) {
                // TODO apply filter 
                // apply projection if the user call addProjection
                if (request.projection.size() == 0) {
                    // want all fields 
                    response.results.push_back(viter->value.share());
                } else {
                    // serialize partial SKVRecord according to projection
                    dto::SKVRecord::Storage storage;
                    bool success = _makeProjection(viter->value, request, storage);
                    if (!success) {
                        K2WARN("Error making projection!");
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

        // Do a push but we need to save our place in the query
        // TODO we can test the filter condition against the WI and last committed version and possibly
        // avoid a push
        // Must update read cache before doing an async operation
        request.reverseDirection ?
            _readCache->insertInterval(key_it->first, request.key, request.mtr.timestamp) :
            _readCache->insertInterval(request.key, key_it->first, request.mtr.timestamp);

        K2DEBUG("About to PUSH in query request");
        return _doPush(request.collectionName, viter->txnId, request.mtr, deadline)
        .then([this, curKey=key_it->first, sitMTR=viter->txnId.mtr, request=std::move(request),
                        resp=std::move(response), deadline](auto&& winnerMTR) mutable {
            if (winnerMTR == sitMTR) {
                // sitting transaction won. Abort the incoming request
                return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in query push"), dto::K23SIQueryResponse{});
            }
            // incoming request won. re-run query logic after removing WI if necessary
            auto& wi_versions = _indexer.find(curKey)->second;
            if (wi_versions.size() > 0 && wi_versions.front().status == dto::DataRecord::WriteIntent) {
                wi_versions.pop_front();
            }
            request.key = curKey;
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
    request.reverseDirection ?
        _readCache->insertInterval(endInterval, request.key, request.mtr.timestamp) :
        _readCache->insertInterval(request.key, endInterval, request.mtr.timestamp);


    response.nextToScan = _getContinuationToken(key_it, request, response.results.size());
    response.exclusiveToken = request.exclusiveKey;
    K2DEBUG("nextToScan: " << response.nextToScan << ", exclusiveToken: " << response.exclusiveToken);
    return RPCResponse(dto::K23SIStatus::OK("Query success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline) {
    K2DEBUG("Partition: " << _partition << ", received read " << request);

    Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIReadResponse{});
    }

    // sitMTR will be ZERO for original requests or non-zero for post-PUSH reads
    // update the read cache to lock out any future writers which may attempt to modify the key range
    // before this read's timestamp
    if (sitMTR == dto::K23SI_MTR_ZERO) {
        _readCache->insertInterval(request.key, request.key, request.mtr.timestamp);
    }

    // find the version deque for the key
    auto fiter = _indexer.find(request.key);
    if (fiter == _indexer.end()) {
        return _makeReadOK(nullptr);
    }
    auto& versions = fiter->second;
    auto viter = versions.begin();
    // position the version iterator at the version we should be returning
    while(viter != versions.end() && request.mtr.timestamp.compareCertain(viter->txnId.mtr.timestamp) < 0) {
        ++viter;
    }
    if (viter == versions.end()) {
        // happy case: we either had no versions, or all versions were newer than the requested timestamp
        return _makeReadOK(nullptr);
    }

    // happy case: either committed, or txn is reading its own write
    if (viter->status == dto::DataRecord::Committed || viter->txnId.mtr == request.mtr) {
        return _makeReadOK(&(*viter));
    }
    // record is still pending and isn't from same transaction.

    if (sitMTR == dto::K23SI_MTR_ZERO) {
        // this is a fresh read finding a WI. have to do a push
        sitMTR = viter->txnId.mtr;
        return _doPush(request.collectionName, viter->txnId, request.mtr, deadline)
            .then([this, sitMTR, request=std::move(request), deadline](auto&& winnerMTR) mutable {
                if (winnerMTR == sitMTR) {
                    // sitting transaction won. Abort the incoming request
                    return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in read push"), dto::K23SIReadResponse{});
                }
                // incoming request won. re-run read logic
                return handleRead(std::move(request), sitMTR, deadline);
            });
    }
    // this is a read after a push and we still find a WI. This WI must be the exact same one we pushed against,
    // or else our read cache was not used correctly in code.
    K2ASSERT(sitMTR == viter->txnId.mtr, "bug in code: found WI after push");
    K2ASSERT(viter == versions.begin(), "must be at newest version if we found a write intent")

    // remove the WI from cache
    versions.pop_front();
    return _makeReadOK(versions.begin() == versions.end() ? nullptr : &(versions[0]));
}

template <typename RequestT>
bool K23SIPartitionModule::_validateStaleWrite(const RequestT& request, std::deque<dto::DataRecord>& versions) {
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        return false;
    }
    // check read cache for R->W conflicts
    auto ts = _readCache->checkInterval(request.key, request.key);
    if (request.mtr.timestamp.compareCertain(ts) < 0) {
        // this key range was read more recently than this write
        K2DEBUG("Partition: " << _partition << ", read cache validation failed for key: " << request.key);
        return false;
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
        K2DEBUG("Partition: " << _partition << ", failing write older than latest commit for key " << request.key);
        return false;
    }
    else if (versions.size() > 1 && versions[0].status == dto::DataRecord::WriteIntent &&
        request.mtr.timestamp.compareCertain(versions[1].txnId.mtr.timestamp) <= 0) {
        // second newest version is the latest committed and its newer than the request.
        // no need to push since this request would fail anyway against the committed value
        K2DEBUG("Partition: " << _partition << ", failing write older than latest commit for key " << request.key);
        return false;
    }

    K2DEBUG("Partition: " << _partition << ", stale write check passed for key " << request.key);
    return true;
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
    dto::Schema& schema = schemaVer->second;

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
        dto::Schema& baseSchema = latestSchemaVer->second;

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
    dto::Schema& schema = schemaVer->second;
    std::vector<bool> excludedFields(schema.fields.size(), true);   // excludedFields for projection
    Payload projectedPayload(Payload::DefaultAllocator);                     // payload for projection

    for (uint32_t i = 0; i < schema.fields.size(); ++i) {
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
K23SIPartitionModule::handleWrite(dto::K23SIWriteRequest&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline) {
    // NB: failures in processing a write do not require that we set the TR state to aborted at the TRH. We rely on
    //     the client to do the correct thing and issue an abort on a failure.
    // NB: sitMTR will be ZERO for original requests or non-zero for post-PUSH, winning writes.
    K2DEBUG("Partition: " << _partition << ", handle write: " << request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2DEBUG("Partition: " << _partition << ", failed validation for " << request.key);
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
        K2DEBUG("Partition: " << _partition << ", designating trh for key " << request.key);
        return _txnMgr.onAction(TxnRecord::Action::onCreate, {.trh=request.trh, .mtr=request.mtr})
        .then([this, request=std::move(request), sitMTR=std::move(sitMTR), deadline]() mutable {
            K2DEBUG("Partition: " << _partition << ", tr created and re-driving request for key " << request.key);
            request.designateTRH = false; // unset the flag and re-run
            return handleWrite(std::move(request), std::move(sitMTR), deadline);
        })
        .handle_exception_type([this](TxnManager::ClientError&) {
            // Failed to create
            K2DEBUG("Partition: " << _partition << ", failed creating TR");
            return RPCResponse(dto::K23SIStatus::AbortConflict("txn too old in write"), dto::K23SIWriteResponse{});
        });
    }

    auto& versions = _indexer[request.key];
    // in this situation, return AbortRequestTooOld error.
    if (!_validateStaleWrite(request, versions)) {
        K2DEBUG("Partition: " << _partition << ", request too old for key " << request.key);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request too old in write"), dto::K23SIWriteResponse{});
    }

    // check to see if we should push or if we're coming after a push and the WI is still here
    if (versions.size() > 0 && versions[0].status == dto::DataRecord::WriteIntent) {
        auto& rec = versions[0];
        auto& rqmtr = request.mtr;

        if (sitMTR == rec.txnId.mtr) {
            K2DEBUG("Partition: " << _partition << ", post-push winner for key " << request.key);
            // this is a post-PUSH request which won over the siting WI and we still have the WI in cache
            versions.pop_front();
        }
        else if (rec.txnId.mtr != rqmtr) {
            // this is a write request finding a WI from a different transaction. Do another push with the remaining
            // deadline time.
            K2DEBUG("Partition: " << _partition << ", different WI found for key " << request.key);
            sitMTR = rec.txnId.mtr;
            return _doPush(request.collectionName, rec.txnId, request.mtr, deadline)
                .then([this, sitMTR, request = std::move(request), deadline](auto&& winnerMTR) mutable {
                    if (winnerMTR == sitMTR) {
                        // sitting transaction won. Abort the incoming request
                        K2DEBUG("Partition: " << _partition << ", push lost for key " << request.key);
                        return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in write push"), dto::K23SIWriteResponse{});
                    }
                    // incoming request won. re-run write logic
                    K2DEBUG("Partition: " << _partition << ", push won for key " << request.key);
                    return handleWrite(std::move(request), sitMTR, deadline);
                });
        }
    }

    if (request.fieldsForPartialUpdate.size() > 0) {
        // parse the partial record to full record
        if ( !versions.size() || versions[0].isTombstone) {
            // cannot parse partial record without a version
            return RPCResponse(dto::K23SIStatus::KeyNotFound("can not partial update with no/deleted version"), dto::K23SIWriteResponse{});
        }
        if (!_parsePartialRecord(request, versions[0])) {
            K2DEBUG("Partition: " << _partition << ", can not parse partial record for key " << request.key);
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
        K2DEBUG("Partition: " << _partition << ", WI created");
        return RPCResponse(dto::K23SIStatus::Created("wi created"), dto::K23SIWriteResponse{});
    });
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
K23SIPartitionModule::handleTxnPush(dto::K23SITxnPushRequest&& request) {
    K2DEBUG("Partition: " << _partition << ", push request: " << request);
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
    // the only state for which we'd directly abort is the Created state (we didn't have this txn)
    bool abortIncumbent = incumbent.state == dto::TxnRecordState::Created;
    if (incumbent.state == dto::TxnRecordState::InProgress) {
        // check the cases when we have to abort the incumbent
        // #1 abort based on priority
        if (incumbent.txnId.mtr.priority > request.challengerMTR.priority) { // bigger number means lower priority
            K2DEBUG("Partition: " << _partition << ", aborting incumbent for key " << request.key);
            abortIncumbent = true;
        }
        // #2 if equal, pick the newer transaction
        else if (incumbent.txnId.mtr.priority == request.challengerMTR.priority) {
            auto cmpResult = incumbent.txnId.mtr.timestamp.compareCertain(request.challengerMTR.timestamp);
            if (cmpResult == dto::Timestamp::LT) {
                K2DEBUG("Partition: " << _partition << ", aborting incumbent for key " << request.key);
                abortIncumbent = true;
            }
            else if (cmpResult == dto::Timestamp::EQ) {
                // #3 if same priority and timestamp, abort on tso ID which must be unique
                if (incumbent.txnId.mtr.timestamp.tsoId() < request.challengerMTR.timestamp.tsoId()) {
                    K2DEBUG("Partition: " << _partition << ", aborting incumbent for key " << request.key);
                    abortIncumbent = true;
                }
                else {
                    // make sure we don't have a bug - the timestamps cannot be the same
                    K2ASSERT(incumbent.txnId.mtr.timestamp.tsoId() != request.challengerMTR.timestamp.tsoId(), "invalid timestamps detected");
                }
            }
        }
    }
    if (abortIncumbent) {
        // challenger won
        return _txnMgr.onAction(TxnRecord::Action::onForceAbort, std::move(txnId)).then([mtr=std::move(request.challengerMTR)] {
            return RPCResponse(dto::K23SIStatus::OK("challenger won in push"), dto::K23SITxnPushResponse{.winnerMTR = std::move(mtr)});
        });
    }
    else {
        // incumbent won
        K2DEBUG("Partition: " << _partition << ", incumbent won for key " << request.key);
        return RPCResponse(dto::K23SIStatus::OK("incumbent won in push"), dto::K23SITxnPushResponse{.winnerMTR = std::move(txnId.mtr)});
    }
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
K23SIPartitionModule::handleTxnEnd(dto::K23SITxnEndRequest&& request) {
    K2DEBUG("Partition: " << _partition << ", transaction end: " << request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2DEBUG("Partition: " << _partition << ", transaction end too old for txn=" << request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in end"), dto::K23SITxnEndResponse());
    }

    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        K2DEBUG("Partition: " << _partition << ", transaction end outside retention for txn=" << request.mtr);
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
            K2DEBUG("Partition: " << _partition << ", transaction ended");
            return RPCResponse(dto::K23SIStatus::OK("transaction ended"), dto::K23SITxnEndResponse());
        })
        .handle_exception_type([this](TxnManager::ClientError&) {
            K2DEBUG("Partition: " << _partition << ", failed transaction end");
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("transaction state transition not allowed in end"), dto::K23SITxnEndResponse());
        });
}

seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
K23SIPartitionModule::handleTxnHeartbeat(dto::K23SITxnHeartbeatRequest&& request) {
    K2DEBUG("Partition: " << _partition << ", transaction hb: " << request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2DEBUG("Partition: " << _partition << ", txn hb too old txn=" << request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in hb"), dto::K23SITxnHeartbeatResponse());
    }
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        K2DEBUG("Partition: " << _partition << ", txn hb too old txn=" << request.mtr);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("txn too old in hb"), dto::K23SITxnHeartbeatResponse());
    }

    return _txnMgr.onAction(TxnRecord::Action::onHeartbeat, dto::TxnId{.trh=std::move(request.key), .mtr=std::move(request.mtr)})
    .then([this]() {
        // heartbeat was applied successfully
        K2DEBUG("Partition: " << _partition << ", txn hb success");
        return RPCResponse(dto::K23SIStatus::OK("hb succeeded"), dto::K23SITxnHeartbeatResponse());
    })
    .handle_exception_type([this] (TxnManager::ClientError&) {
        // there was a problem applying the heartbeat due to client's view of the TR state. Client should abort
        K2DEBUG("Partition: " << _partition << ", txn hb fail");
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("hb not allowed for the txn state"), dto::K23SITxnHeartbeatResponse{});
    });
}

seastar::future<dto::K23SI_MTR>
K23SIPartitionModule::_doPush(String collectionName, dto::TxnId sitTxnId, dto::K23SI_MTR pushMTR, FastDeadline deadline) {
    K2DEBUG("partition: " << _partition << ", executing push against txnid=" << sitTxnId << ", for mtr=" << pushMTR);
    dto::K23SITxnPushRequest request{};
    request.collectionName = std::move(collectionName);
    request.incumbentMTR = std::move(sitTxnId.mtr);
    request.key = std::move(sitTxnId.trh);
    request.challengerMTR = std::move(pushMTR);
    return seastar::do_with(std::move(request), [this, deadline] (auto& request) {
        return _cpo.PartitionRequest<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse, dto::Verbs::K23SI_TXN_PUSH>(deadline, request)
        .then([this](auto&& responsePair) {
            auto& [status, response] = responsePair;
            K2DEBUG("Push request completed with status=" << status << ", and response=" << response);
            if (status != dto::K23SIStatus::OK) {
                K2ERROR("Partition: " << _partition << ", txn push failed");
                return seastar::make_exception_future<dto::K23SI_MTR>(TxnManager::ServerError());
            }
            return seastar::make_ready_future<dto::K23SI_MTR>(std::move(response.winnerMTR));
        });
    });
}

seastar::future<>
K23SIPartitionModule::_createWI(dto::K23SIWriteRequest&& request, std::deque<dto::DataRecord>& versions, FastDeadline deadline) {
    K2DEBUG("Partition: " << _partition << ", Write Request creating WI: " << request);
    dto::DataRecord rec;
    rec.key = std::move(request.key);
    // we need to copy this data into a new memory block so that we don't hold onto and fragment the transport memory
    rec.value = request.value.copy();
    rec.isTombstone = request.isDelete;
    rec.txnId = dto::TxnId{.trh = std::move(request.trh), .mtr = std::move(request.mtr)};
    rec.status = dto::DataRecord::WriteIntent;

    versions.push_front(std::move(rec));
    // TODO write to WAL
    return _persistence.makeCall(versions.front(), deadline);
}

seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
K23SIPartitionModule::handleTxnFinalize(dto::K23SITxnFinalizeRequest&& request) {
    // find the version deque for the key
    K2DEBUG("Partition: " << _partition << ", txn finalize: " << request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in finalize"), dto::K23SITxnFinalizeResponse());
    }
    if (!_validateRequestPartitionKey(request)){
        // do not allow empty partition key
        return RPCResponse(dto::K23SIStatus::BadParameter("missing partition key in finalize"), dto::K23SITxnFinalizeResponse());
    }
    auto fiter = _indexer.find(request.key);
    if (fiter == _indexer.end() || fiter->second.empty()) {
        if (request.action == dto::EndAction::Abort) {
            // we don't have it but it was an abort anyway
            K2DEBUG("Partition: " << _partition << ", abort for missing key " << request.key << ", in txn " << request.mtr);
            return RPCResponse(dto::K23SIStatus::OK("finalize key missing in abort"), dto::K23SITxnFinalizeResponse());
        }
        // we can't allow the commit since we don't have the write intent
        K2DEBUG("Partition: " << _partition << ", rejecting commit for missing key " << request.key << ", in txn " << request.mtr);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("cannot commit missing key"), dto::K23SITxnFinalizeResponse());
    }
    auto& versions = fiter->second;
    auto viter = versions.begin();
    // position the version iterator at the version we should be converting
    while (viter != versions.end() && request.mtr.timestamp.compareCertain(viter->txnId.mtr.timestamp) < 0) {
        ++viter;
    }

    dto::TxnId txnId{.trh=std::move(request.trh), .mtr=std::move(request.mtr)};
    if (viter == versions.end() || viter->txnId != txnId) {
        // we don't have a record from this transaction
        if (request.action == dto::EndAction::Abort) {
            // we don't have it but it was an abort anyway
            K2DEBUG("Partition: " << _partition << ", abort for missing version " << request.key << ", in txn " << request.mtr);
            return RPCResponse(dto::K23SIStatus::OK("finalize key missing in abort"), dto::K23SITxnFinalizeResponse());
        }
        // we can't allow the commit since we don't have the write intent and we don't have a committed version
        K2DEBUG("Partition: " << _partition << ", rejecting commit for missing version " << request.key << ", in txn " << request.mtr);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("cannot commit missing key"), dto::K23SITxnFinalizeResponse());
    }

    // we found a record from this transaction
    if (viter->status != dto::DataRecord::WriteIntent) {
        // asked to commit and it was already committed
        if (request.action == dto::EndAction::Commit) {
            // we have it committed already
            K2DEBUG("Partition: " << _partition << ", committed already " << request.key << ", in txn " << request.mtr);
            return RPCResponse(dto::K23SIStatus::OK("already committed"), dto::K23SITxnFinalizeResponse());
        }
        // we can't allow the abort since the record is already committed
        K2DEBUG("Partition: " << _partition << ", failing abort for committed already " << request.key << ", in txn " << request.mtr);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("cannot abort committed txn"), dto::K23SITxnFinalizeResponse());
    }

    // it is a write intent
    if (request.action == dto::EndAction::Commit) {
        K2DEBUG("Partition: " << _partition << ", committing " << request.key << ", in txn " << request.mtr);
        viter->status = dto::DataRecord::Committed;
    }
    else {
        K2DEBUG("Partition: " << _partition << ", aborting " << request.key << ", in txn " << request.mtr);
        // erase from version list
        versions.erase(viter);
        if (versions.empty()) {
            // if there are no versions left, erase the key from indexer
            _indexer.erase(fiter);
        }
    }
    // send a partiall update
    return _persistence.makeCall(dto::K23SI_PersistencePartialUpdate{}, _config.persistenceTimeout()).then([]{
        return RPCResponse(dto::K23SIStatus::OK("persistence call succeeded"), dto::K23SITxnFinalizeResponse{});
    });
}

seastar::future<std::tuple<Status, dto::K23SIPushSchemaResponse>>
K23SIPartitionModule::handlePushSchema(dto::K23SIPushSchemaRequest&& request) {
    K2DEBUG("handlePushSchema for schema: " << request.schema.name);
    if (_cmeta.name != request.collectionName) {
        return RPCResponse(Statuses::S403_Forbidden("Collection names in partition and request do not match"), dto::K23SIPushSchemaResponse{});
    }

    _schemas[request.schema.name][request.schema.version] = std::move(request.schema);

    return RPCResponse(Statuses::S200_OK("push schema success"), dto::K23SIPushSchemaResponse{});
}

// For test and debug purposes, not normal transaction processsing
// Returns all versions+WIs for a particular key
seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
K23SIPartitionModule::handleInspectRecords(dto::K23SIInspectRecordsRequest&& request) {
    K2DEBUG("handleInspectRecords for: " << request.key);

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
    K2DEBUG("handleInspectTxn key: " << request.key << ", mtr: " << request.mtr);

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
    K2DEBUG("handleInspectWIs");
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
    K2DEBUG("handleInspectAllTxns");

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
    K2DEBUG("handleInspectAllKeys");
    std::vector<dto::Key> keys;
    keys.reserve(_indexer.size());

    for (auto it = _indexer.begin(); it != _indexer.end(); ++it) {
        keys.push_back(it->first);
    }

    dto::K23SIInspectAllKeysResponse response { std::move(keys) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect AllKeys success"), std::move(response));
}

} // ns k2
