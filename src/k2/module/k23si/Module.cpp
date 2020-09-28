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
    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse>(dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
        return handleRead(std::move(request), dto::K23SI_MTR_ZERO, FastDeadline(_config.readTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest&& request) {
        return handleWrite(std::move(request), dto::K23SI_MTR_ZERO, FastDeadline(_config.writeTimeout()));
    });

    RPC().registerRPCObserver<dto::K23SIPartialUpdateRequest, dto::K23SIPartialUpdateResponse>(dto::Verbs::K23SI_PARTIAL_UPDATE, [this](dto::K23SIPartialUpdateRequest&& request) {
        return handlePartialUpdate(std::move(request), dto::K23SI_MTR_ZERO, FastDeadline(_config.writeTimeout()));
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

seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline) {
    K2DEBUG("Partition: " << _partition << ", received read " << request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in read"), dto::K23SIReadResponse{});
    }
    if (!_validateRequestParameter(request)){
        // do not allow empty partition key
        return RPCResponse(dto::K23SIStatus::BadParameter("missing partition key in read"), dto::K23SIReadResponse{});
    }
    if (!_validateRetentionWindow(request)) {
        // the request is outside the retention window
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request too old in read"), dto::K23SIReadResponse{});
    }
    if (_schemas.find(request.key.schemaName) == _schemas.end()) {
        // server does not have schema
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("schema does not exist"), dto::K23SIReadResponse{});
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

    // remove the WI from cache and queue it up for cleanup
    _queueWICleanup(std::move(*viter));
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

bool K23SIPartitionModule::_copyBaseToPayload(dto::SKVRecord::Storage& storage, Payload& payload, dto::FieldType type) {    
    switch (type) {
    case k2::dto::FieldType::STRING : {
        k2::String value;
        bool success = storage.fieldData.read(value);
        if (!success) {
            throw new std::runtime_error("_parsePartialRecord() read latest version of SKVRecord failed");
            return false;
        }
        payload.write(value);
        std::cout << "{excluded: print dst SKVRecord} FieldType:" << type << ", value:" << value << std::endl;
        break;
    }
    case k2::dto::FieldType::UINT32T : {
        break;
    }
    case k2::dto::FieldType::UINT64T : {
        break;
    }
    default :
        throw new std::runtime_error("_parsePartialRecord schema field type not correct");
        return false;
    }// end switch
    return true;
}

bool K23SIPartitionModule::_copyUpdateToPayload(dto::SKVRecord::Storage& readButIgnore, dto::SKVRecord::Storage& storage, Payload& payload, dto::FieldType type) {    
    switch (type) {
    case k2::dto::FieldType::STRING : {
        k2::String value;
        bool success = readButIgnore.fieldData.read(value); // in order to _advancePosition of base payload
        if (!success) {
            throw new std::runtime_error("_parsePartialRecord() read latest version of SKVRecord failed");
            return false;
        }
        std::cout << "{included: print origin SKVRecord} type:" << type << ", value:" << value << std::endl;
        success = storage.fieldData.read(value);
        if (!success) {
            return false;
        }
        std::cout << "{included: print dst SKVRecord} type:" << type << ", value:" << value << std::endl;
        payload.write(value);
        break;
    }
    case k2::dto::FieldType::UINT32T : {
        break;
    }
    case k2::dto::FieldType::UINT64T : {
        break;
    }
    default :
        throw new std::runtime_error("_parsePartialRecord schema field type not correct");
        return false;
    }// end switch
    return true;
}

template <typename T>
void K23SIPartitionModule::_copyMapToPayload(std::unordered_map<std::size_t, T>& map, std::size_t fieldIndex, Payload& payload) {
    T value;
    auto mapIt = map.find(fieldIndex);
    if ( mapIt == map.end() ) {
        throw new std::runtime_error("_copyMapToPayload cannot find value from map");
    }
    value = mapIt->second;
    payload.write(value);
}

template <typename T>
bool K23SIPartitionModule::_copyBaseToMap(dto::SKVRecord::Storage& storage, std::size_t fieldIndex, std::unordered_map<std::size_t, T>& map) {
    T value;
    bool success = storage.fieldData.read(value); 
    if (!success) {
        return false;
    }
    map.insert ({fieldIndex, value});
    return true;
}

bool K23SIPartitionModule::_parsePartialRecord(dto::SKVRecord::Storage& storage, const k2::String& schemaName, std::deque<dto::DataRecord>& versions) {
    // cannot parse partial record without a version
    if (versions.size() == 0) return false;

    auto schemaIt = _schemas.find(schemaName);
    auto schemaVer = schemaIt->second.find(storage.schemaVersion);
    dto::Schema schema = schemaVer->second;
    Payload payload([&] { return Binary(4096); });

    // based on the latest version
    if (storage.schemaVersion == versions[0].value.schemaVersion) { 
        // quick path --same schema version.
        for (std::size_t i = 0; i < schema.fields.size(); ++i) {
            if (storage.excludedFields[i]) {
                // use 'base skvrecord' value
                if (!_copyBaseToPayload(versions[0].value, payload, schema.fields[i].type)) return false;
            } else {
                // use 'storage' value
                if (!_copyUpdateToPayload(versions[0].value, storage, payload, schema.fields[i].type)) return false;
            }
        }
        storage.fieldData = std::move(payload);
        storage.fieldData.truncateToCurrent();
        std::cout << "storage.fieldData.size()=" << storage.fieldData.getSize() << std::endl;
    } else { 
        // slow path--different schema version. 
        auto lSchemaVer = schemaIt->second.find(versions[0].value.schemaVersion);
        dto::Schema baseSchema = lSchemaVer->second;
        std::unordered_map<std::size_t, k2::String> mapString; // unordered_map temporarily stores base SKVRecords
        std::unordered_map<std::size_t, uint32_t> mapUint32;
        std::unordered_map<std::size_t, uint64_t> mapUint64;
        std::size_t baseCursor = 0; // indicate read cursor for base payload
        std::size_t findField;
        for (std::size_t i = 0; i < schema.fields.size(); ++i) {
            findField = -1;
            if (storage.excludedFields[i]) {
                // payload value comes from base SKVRecord
                // find index of the field in base payload
                findField = _findField(baseSchema, schema.fields[i].name, schema.fields[i].type);
                if (findField == (std::size_t)-1) {
                    return false; // if do not find any field, error return
                } 

                if (findField < baseCursor) {
                    // Every fields' value whose index is lower than baseCursor is save in the unordered_maps
                    // write() the i-th field to WI full record from unordered_map
                    switch (baseSchema.fields[findField].type){
                    case dto::FieldType::STRING: {
                        _copyMapToPayload(mapString, findField, payload);
                        break;
                    }
                    case dto::FieldType::UINT32T : {
                        _copyMapToPayload(mapUint32, findField, payload);
                        break;
                    }
                    case dto::FieldType::UINT64T : {
                        _copyMapToPayload(mapUint64, findField, payload);
                        break;
                    }
                    default :
                        throw new std::runtime_error("_parsePartialRecord schema field type not correct");
                        break;
                    } // end switch
                } else {
                    // 1. save value in unordered_map from baseCursor to (findField-1) according to base SKVRecord;
                    // 2. write 'findField' value from base SKVRecord to field 'i' of WI full record
                    // 3. baseCursor = findField + 1
                    for (; baseCursor < findField; ++baseCursor) {
                        switch (baseSchema.fields[findField].type) {
                        case dto::FieldType::STRING: {
                            if(!_copyBaseToMap(versions[0].value, baseCursor, mapString)) return false;
                            break;
                        }
                        case dto::FieldType::UINT32T: {
                            if(!_copyBaseToMap(versions[0].value, baseCursor, mapUint32)) return false;
                            break;
                        }
                        case dto::FieldType::UINT64T: {
                            if(!_copyBaseToMap(versions[0].value, baseCursor, mapUint64)) return false;
                            break;
                        }
                        default :
                            throw new std::runtime_error("_parsePartialRecord schema field type not correct");
                            break;
                        } // end switch
                    }

                    if (!_copyBaseToPayload(versions[0].value, payload, baseSchema.fields[findField].type)) return false;
                    baseCursor = findField + 1;
                }
            } else {
                // Payload value comes from partial update -- 'storage'
                if (!_copyBaseToPayload(storage, payload, schema.fields[i].type)) return false;
            }
        }
    }

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
    if (!_validateRequestParameter(request)){
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
            std::cout << "{_queueWICleanup} mtr:" << sitMTR << std::endl;
            _queueWICleanup(std::move(rec));
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

    // debug
    if (versions.size() > 0) {
        for(uint32_t i = 0; i < versions.size(); ++i) {
            std::cout << "{Print VERSIONS} key:" << versions[i].key << ", status:" << versions[i].status 
                    << ", mtr:" << versions[i].txnId.mtr << std::endl;
        }
    }
    
    // Clean up if req and WI are the same transaction
    if (versions.size() > 0 && versions[0].status == dto::DataRecord::WriteIntent && request.mtr == versions[0].txnId.mtr) {
        _queueWICleanup(std::move(versions[0]));
        versions.pop_front();
    }

    // all checks passed - we're ready to place this WI as the latest version(at head of versions deque)
    return _createWI(std::move(request), versions, deadline).then([this]() mutable {
        K2DEBUG("Partition: " << _partition << ", WI created");
        return RPCResponse(dto::K23SIStatus::Created("wi created"), dto::K23SIWriteResponse{});
    });
}

seastar::future<std::tuple<Status, dto::K23SIPartialUpdateResponse>>
K23SIPartitionModule:: handlePartialUpdate(dto::K23SIPartialUpdateRequest&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline) {
    K2DEBUG("Partition: " << _partition << ", handle partial update: " << request);
    std::cout << "{Module::handlePartialUpdate} Partition: " << _partition << ", handle partial update: " << request << std::endl;
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2DEBUG("Partition: " << _partition << ", failed validation for " << request.key);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in partial update"), dto::K23SIPartialUpdateResponse{});
    }
    if (!_validateRequestParameter(request)){
        // do not allow empty partition key
        return RPCResponse(dto::K23SIStatus::BadParameter("missing partition key in partial update"), dto::K23SIPartialUpdateResponse{});
    }

    auto schemaIt = _schemas.find(request.key.schemaName);
    if (schemaIt == _schemas.end()) {
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("schema does not exist"), dto::K23SIPartialUpdateResponse{});
    }
    if (schemaIt->second.find(request.value.schemaVersion) == schemaIt->second.end()) {
        // server does not have schema
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("schema does not exist"), dto::K23SIPartialUpdateResponse{});
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
            return handlePartialUpdate(std::move(request), std::move(sitMTR), deadline);
        })
        .handle_exception_type([this](TxnManager::ClientError&) {
            // Failed to create
            K2DEBUG("Partition: " << _partition << ", failed creating TR");
            return RPCResponse(dto::K23SIStatus::AbortConflict("txn too old in write"), dto::K23SIPartialUpdateResponse{});
        });
    }

    auto& versions = _indexer[request.key];
    if (!_validateStaleWrite(request, versions)) {
        K2DEBUG("Partition: " << _partition << ", request too old for key " << request.key);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request too old in write"), dto::K23SIPartialUpdateResponse{});
    }

    // check to see if we should push or if we're coming after a push and the WI is still here
    if (versions.size() > 0 && versions[0].status == dto::DataRecord::WriteIntent) {
        auto& rec = versions[0];
        auto& rqmtr = request.mtr;

        if (sitMTR == rec.txnId.mtr) {
            K2DEBUG("Partition: " << _partition << ", post-push winner for key " << request.key);
            // this is a post-PUSH request which won over the siting WI and we still have the WI in cache
            _queueWICleanup(std::move(rec));
            versions.pop_front();
        }
        else if (rec.txnId.mtr != rqmtr) {
            // this is a update request finding a WI from a different transaction. Do another push with the remaining
            // deadline time.
            K2DEBUG("Partition: " << _partition << ", different WI found for key " << request.key);
            sitMTR = rec.txnId.mtr;
            return _doPush(request.collectionName, rec.txnId, request.mtr, deadline)
                .then([this, sitMTR, request = std::move(request), deadline](auto&& winnerMTR) mutable {
                    if (winnerMTR == sitMTR) {
                        // sitting transaction won. Abort the incoming request
                        K2DEBUG("Partition: " << _partition << ", push lost for key " << request.key);
                        return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in update push"), dto::K23SIPartialUpdateResponse{});
                    }
                    // incoming request won. re-run write logic
                    K2DEBUG("Partition: " << _partition << ", push won for key " << request.key);
                    return handlePartialUpdate(std::move(request), sitMTR, deadline);
                });
        }
    }

    // parse the partial record to full record
    if (!_parsePartialRecord(request.value, request.key.schemaName, versions)) {
        K2DEBUG("Partition: " << _partition << ", can not parse partial record for key " << request.key);
        return RPCResponse(dto::K23SIStatus::OperationNotAllowed("can not parse partial record"), dto::K23SIPartialUpdateResponse{});
    }

    // Clean up if req and WI are the same transaction
    if (versions.size() > 0 && versions[0].status == dto::DataRecord::WriteIntent && request.mtr == versions[0].txnId.mtr) {
        _queueWICleanup(std::move(versions[0]));
        versions.pop_front();
    }
    
    // all checks passed - we're ready to place this WI as the latest version(at head of versions deque)
    return _createWI(std::move(request), versions, deadline).then([this]() mutable {
        K2DEBUG("Partition: " << _partition << ", WI created");
        return RPCResponse(dto::K23SIStatus::Created("wi created"), dto::K23SIPartialUpdateResponse{});
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

void K23SIPartitionModule::_queueWICleanup(dto::DataRecord&& rec) {
    dto::DataRecord(std::move(rec)); // move the record here so that we can drop it
}

template <typename RequestT>
seastar::future<>
K23SIPartitionModule::_createWI(RequestT&& request, std::deque<dto::DataRecord>& versions, FastDeadline deadline) {
    K2DEBUG("Partition: " << _partition << ", creating WI: " << request);
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
    if (!_validateRequestParameter(request)){
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
