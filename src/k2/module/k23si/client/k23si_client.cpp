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

#include "k23si_client.h"
#include "query.h"

namespace k2 {

K2TxnHandle::K2TxnHandle(dto::K23SI_MTR&& mtr, K2TxnOptions options, CPOClient* cpo, K23SIClient* client, Duration d, TimePoint start_time) noexcept : _mtr(std::move(mtr)), _options(std::move(options)), _cpo_client(cpo), _client(client), _valid(true), _failed(false), _failed_status(Statuses::S200_OK("default fail status")), _txn_end_deadline(d), _start_time(start_time) {
    K2LOG_D(log::skvclient, "ctor, mtr={}", _mtr);
}

void K2TxnHandle::_checkResponseStatus(Status& status) {
    if (status == dto::K23SIStatus::AbortConflict ||
        status == dto::K23SIStatus::AbortRequestTooOld ||
        status == dto::K23SIStatus::InternalError ||
        status == dto::K23SIStatus::OperationNotAllowed) {
        _failed = true;
        _failed_status = status;
    }

    if (status == dto::K23SIStatus::AbortConflict) {
        _client->abort_conflicts++;
    }

    if (status == dto::K23SIStatus::AbortRequestTooOld) {
        _client->abort_too_old++;
        K2LOG_D(log::skvclient, "Abort: txn too old: mtr={}", _mtr);
    }
}

void K2TxnHandle::_makeHeartbeatTimer() {
    K2LOG_D(log::skvclient, "makehb, mtr={}", _mtr);
    _heartbeat_timer.setCallback([this] {
        _client->heartbeats++;

        auto request = std::make_unique<dto::K23SITxnHeartbeatRequest>(
            dto::K23SITxnHeartbeatRequest {
                .pvid{}, // will be filled by PartitionRequest
                .collectionName=_trh_collection,
                .key=_trh_key.value(),
                .mtr=_mtr
            });

        K2LOG_D(log::skvclient, "send hb for mtr={}", _mtr);

        return _cpo_client->partitionRequest<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse, dto::Verbs::K23SI_TXN_HEARTBEAT>(Deadline(_heartbeat_interval), *request)
        .then([this] (auto&& response) {
            auto& [status, k2response] = response;
            _checkResponseStatus(status);
            if (_failed) {
                K2LOG_D(log::skvclient, "txn failed: cancelling hb in mtr={}", _mtr);
                _heartbeat_timer.cancel();
            }
        }).finally([request=std::move(request)] {
            (void)request;
            // Memory freed after request is out of scope
        });
    });
}

std::unique_ptr<dto::K23SIReadRequest> K2TxnHandle::_makeReadRequest(
                        const dto::Key& key, const String& collection) const {
    return std::make_unique<dto::K23SIReadRequest>(
        dto::PVID{}, // Will be filled in by PartitionRequest
        collection,
        _mtr,
        key
    );
}

template <>
seastar::future<ReadResult<dto::SKVRecord>> K2TxnHandle::read(dto::SKVRecord record) {
    for (const String& key : record.partitionKeys) {
        if (key == "") {
            return seastar::make_exception_future<ReadResult<dto::SKVRecord>>(K23SIClientException("Partition key field not set for read request"));
        }
    }
    for (const String& key : record.rangeKeys) {
        if (key == "") {
            return seastar::make_exception_future<ReadResult<dto::SKVRecord>>(K23SIClientException("Range key field not set for read request"));
        }
    }

    return read(record.getKey(), record.collectionName);
}

seastar::future<ReadResult<dto::SKVRecord>> K2TxnHandle::read(dto::Key key, String collection) {
    if (!_valid) {
        return seastar::make_exception_future<ReadResult<dto::SKVRecord>>(
                K23SIClientException("Invalid use of K2TxnHandle"));
    }
    if (_failed) {
        return seastar::make_ready_future<ReadResult<dto::SKVRecord>>(
                ReadResult<dto::SKVRecord>(_failed_status, dto::SKVRecord()));
    }

    K2LOG_D(log::skvclient, "making request for: schema={}, collection={}", key.schemaName, collection);
    std::unique_ptr<dto::K23SIReadRequest> request = _makeReadRequest(key, collection);

    _client->read_ops++;
    _ongoing_ops++;

    return _cpo_client->partitionRequest
        <dto::K23SIReadRequest, dto::K23SIReadResponse, dto::Verbs::K23SI_READ>
        (_options.deadline, *request).
        then([this, schemaName=std::move(key.schemaName), &collName=request->collectionName] (auto&& response) {
            auto& [status, k2response] = response;
            _checkResponseStatus(status);
            _ongoing_ops--;

            K2LOG_D(log::skvclient, "got status={}", status);
            if (!status.is2xxOK()) {
                return seastar::make_ready_future<ReadResult<dto::SKVRecord>>(
                            ReadResult<dto::SKVRecord>(std::move(status), SKVRecord()));
            }

            return _client->getSchema(collName, schemaName, k2response.value.schemaVersion)
            .then([s=std::move(status), storage=std::move(k2response.value), &collName] (auto&& response) mutable {
                auto& [status, schema_ptr] = response;
                K2LOG_D(log::skvclient, "got status for getSchema: {}", status);

                if (!status.is2xxOK()) {
                    return seastar::make_ready_future<ReadResult<dto::SKVRecord>>(
                        ReadResult<dto::SKVRecord>(
                        dto::K23SIStatus::OperationNotAllowed("Matching schema could not be found"),
                        SKVRecord()));
                }

                SKVRecord skv_record(collName, schema_ptr, std::move(storage), true);
                return seastar::make_ready_future<ReadResult<dto::SKVRecord>>(
                        ReadResult<dto::SKVRecord>(std::move(s), std::move(skv_record)));
            });
        }).finally([r = std::move(request)] () { (void)r; });
}


std::unique_ptr<dto::K23SIWriteRequest> K2TxnHandle::_makeWriteRequest(dto::SKVRecord& record, bool erase,
                                                                      bool rejectIfExists) {
    for (const String& key : record.partitionKeys) {
        if (key == "") {
            throw K23SIClientException("Partition key field not set for write request");
        }
    }
    for (const String& key : record.rangeKeys) {
        if (key == "") {
            throw K23SIClientException("Range key field not set for read request");
        }
    }

    dto::Key key = record.getKey();

    bool isTRH = !_trh_key.has_value();
    if (isTRH) {
        _trh_key = key;
        _trh_collection = record.collectionName;
    }
    return std::make_unique<dto::K23SIWriteRequest>(
        dto::PVID{}, // Will be filled in by PartitionRequest
        record.collectionName,
        _mtr,
        _trh_key.value(),
        _trh_collection,
        erase,
        isTRH,
        rejectIfExists,
        _client->write_ops,
        key,
        record.storage.share(),
        std::vector<uint32_t>()
    );
}

std::unique_ptr<dto::K23SIWriteRequest> K2TxnHandle::_makePartialUpdateRequest(dto::SKVRecord& record,
                    std::vector<uint32_t> fieldsForPartialUpdate, dto::Key&& key) {
        bool isTRH = !_trh_key.has_value();
        if (isTRH) {
            _trh_key = key;
            _trh_collection = record.collectionName;
        }

        return std::make_unique<dto::K23SIWriteRequest>(dto::K23SIWriteRequest{
            dto::PVID{}, // Will be filled in by PartitionRequest
            record.collectionName,
            _mtr,
            _trh_key.value(),
            _trh_collection,
            false, // Partial update cannot be a delete
            isTRH,
            false, // Partial update must be applied on existing record
            _client->write_ops,
            std::move(key),
            record.storage.share(),
            fieldsForPartialUpdate
        });
    }

seastar::future<EndResult> K2TxnHandle::end(bool shouldCommit) {
    if (!_valid) {
        return seastar::make_exception_future<EndResult>(K23SIClientException("Tried to end() an invalid TxnHandle"));
    }
    // User is not allowed to call anything else on this TxnHandle after end()
    _valid = false;

    if (_ongoing_ops != 0) {
        return seastar::make_exception_future<EndResult>(K23SIClientException("Tried to end() with ongoing ops"));
    }

    if (_write_ranges.empty()) {
        _client->successful_txns++;

        if (_failed && shouldCommit) {
            // This means a bug in the application because there is no heartbeat for a RO txn,
            // so the app should have seen the failure on a read and aborted
            K2LOG_W(log::skvclient, "Tried to commit a failed RO transaction, mtr={}", _mtr);
            return seastar::make_ready_future<EndResult>(EndResult(_failed_status));
        }

        return seastar::make_ready_future<EndResult>(EndResult(Statuses::S200_OK("default end result")));
    }

    auto* request  = new dto::K23SITxnEndRequest {
        dto::PVID{}, // Will be filled in by PartitionRequest
        _trh_collection,
        _trh_key.value(),
        _mtr,
        shouldCommit && !_failed ? dto::EndAction::Commit : dto::EndAction::Abort,
        std::move(_write_ranges),
        _options.syncFinalize
    };

    K2LOG_D(log::skvclient, "Cancel hb for {}", _mtr);
    _heartbeat_timer.cancel();

    return _cpo_client->partitionRequest
        <dto::K23SITxnEndRequest, dto::K23SITxnEndResponse, dto::Verbs::K23SI_TXN_END>
        (Deadline<>(_txn_end_deadline), *request).
        then([this, shouldCommit] (auto&& response) {
            auto& [status, k2response] = response;
            if (status.is2xxOK() && !_failed) {
                _client->successful_txns++;
            } else if (!status.is2xxOK()){
                K2LOG_W(log::skvclient, "TxnEndRequest failed: status={}, mtr={}", status, _mtr);
            }

            if (_failed && shouldCommit && status.is2xxOK()) {
                // This could either be a bug in the application, where the failure was a read/write op
                // and the app should know to abort, or it is a normal scenario where there was a
                // failure on the heartbeat and the app was not aware.
                //
                // Either way, we aborted the transaction but we need to indicate to the app that
                // it was not commited
                K2LOG_D(log::skvclient, "Tried to commit a failed transaction, mtr={}", _mtr);
                status = _failed_status;
            }

            return _heartbeat_timer.stop().then([this, s=std::move(status)] () {
                // TODO get min transaction time from TSO client
                auto time_spent = Clock::now() - _start_time;
                if (time_spent < 50us) {
                    auto sleep = 50us - time_spent;
                    return seastar::sleep(sleep).then([s=std::move(s)] () {
                        return seastar::make_ready_future<EndResult>(EndResult(std::move(s)));
                    });
                }

                return seastar::make_ready_future<EndResult>(EndResult(std::move(s)));
            });
        }).finally([request] () { delete request; });
}

seastar::future<WriteResult> K2TxnHandle::erase(SKVRecord& record) {
    return write(record, true);
}

K23SIClient::K23SIClient(const K23SIClientConfig &) :
        _tsoClient(AppBase().getDist<TSO_ClientLib>().local()) {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;
    _metric_groups.add_group("K23SI_client", {
        sm::make_counter("read_ops", read_ops, sm::description("Total K23SI Read operations"), labels),
        sm::make_counter("write_ops", write_ops, sm::description("Total K23SI Write/Delete operations"), labels),
        sm::make_counter("total_txns", total_txns, sm::description("Total K23SI transactions began"), labels),
        sm::make_counter("successful_txns", successful_txns, sm::description("Total K23SI transactions ended successfully (committed or user aborted)"), labels),
        sm::make_counter("abort_conflicts", abort_conflicts, sm::description("Total K23SI transactions aborted due to conflict"), labels),
        sm::make_counter("abort_too_old", abort_too_old, sm::description("Total K23SI transactions aborted due to retention window expiration"), labels),
        sm::make_counter("heartbeats", heartbeats, sm::description("Total K23SI transaction heartbeats sent"), labels),
    });
}

seastar::future<> K23SIClient::start() {
    for (auto it = _tcpRemotes().begin(); it != _tcpRemotes().end(); ++it) {
        _k2endpoints.push_back(String(*it));
    }
    K2LOG_I(log::skvclient, "_cpo={}", _cpo());
    cpo_client.init(_cpo());

    return seastar::make_ready_future<>();
}

seastar::future<> K23SIClient::gracefulStop() {
    return seastar::make_ready_future<>();
}

seastar::future<Status> K23SIClient::makeCollection(const String& collection, std::vector<String>&& rangeEnds) {
    std::vector<String> endpoints = _k2endpoints;
    dto::HashScheme scheme = rangeEnds.size() ? dto::HashScheme::Range : dto::HashScheme::HashCRC32C;

    dto::CollectionMetadata metadata{
        .name = collection,
        .hashScheme = scheme,
        .storageDriver = dto::StorageDriver::K23SI,
        .capacity = {},
        .retentionPeriod = Duration(retention_window())
    };

    return makeCollection(std::move(metadata), std::move(endpoints), std::move(rangeEnds));
}

seastar::future<Status> K23SIClient::makeCollection(dto::CollectionMetadata&& metadata, std::vector<String>&& endpoints, std::vector<String>&& rangeEnds) {
    return cpo_client.createAndWaitForCollection(Deadline<>(create_collection_deadline()), std::move(metadata), std::move(endpoints), std::move(rangeEnds));
}

seastar::future<K2TxnHandle> K23SIClient::beginTxn(const K2TxnOptions& options) {
    auto start_time = Clock::now();
    return _tsoClient.getTimestampFromTSO(start_time)
    .then([this, start_time, options] (auto&& timestamp) {
        dto::K23SI_MTR mtr{
            .timestamp=std::move(timestamp),
            .priority=options.priority
        };

        total_txns++;
        return seastar::make_ready_future<K2TxnHandle>(K2TxnHandle(std::move(mtr), std::move(options), &cpo_client, this, txn_end_deadline(), start_time));
    });
}

seastar::future<CreateSchemaResult> K23SIClient::createSchema(const String& collectionName, dto::Schema schema) {
    return cpo_client.createSchema(collectionName, std::move(schema)).then([](auto&& status) {
        return CreateSchemaResult{.status=std::move(status)};
    });
}

seastar::future<Status> K23SIClient::refreshSchemaCache(const String& collectionName) {
    return cpo_client.getSchemas(collectionName)
    .then([this, collectionName] (auto&& response) {
        auto& [status, collSchemas] = response;

        if (!status.is2xxOK()) {
            K2LOG_W(log::cpoclient, "Failed to refresh schemas from CPO: status={}", status);
            return status;
        }

        auto& schemaMap = schemas[collectionName];
        for (const Schema& schema : collSchemas) {
            schemaMap[schema.name][schema.version] = std::make_shared<dto::Schema>(schema);
        }

        return status;
    });
}

seastar::future<GetSchemaResult> K23SIClient::getSchema(const String& collectionName, const String& schemaName, int64_t schemaVersion) {
    return getSchemaInternal(collectionName, schemaName, schemaVersion, true).then([](auto&& result) {
        auto&& [status, schema] = std::move(result);
        return GetSchemaResult{.status=std::move(status), .schema=std::move(schema)};
    });
}

seastar::future<std::tuple<Status, std::shared_ptr<dto::Schema>>> K23SIClient::getSchemaInternal(const String& collectionName, const String& schemaName, int64_t schemaVersion, bool doCPORefresh) {
    auto cIt = schemas.find(collectionName);
    if (cIt == schemas.end() && !doCPORefresh) {
        return RPCResponse(Statuses::S404_Not_Found("Could not find schema after CPO refresh"), std::shared_ptr<dto::Schema>());
    } else if (cIt == schemas.end()) {
        return refreshSchemaCache(collectionName)
        .then([this, collectionName, schemaName, schemaVersion] (Status&& status) {
            if (!status.is2xxOK()) {
                return RPCResponse(std::move(status), std::shared_ptr<dto::Schema>());
            }

            return getSchemaInternal(collectionName, schemaName, schemaVersion, false);
        });
    }

    auto sIt = cIt->second.find(schemaName);
    if (sIt == cIt->second.end() && !doCPORefresh) {
        return RPCResponse(Statuses::S404_Not_Found("Could not find schema after CPO refresh"), std::shared_ptr<dto::Schema>());
    } else if (sIt == cIt->second.end()) {
        return refreshSchemaCache(collectionName)
        .then([this, collectionName, schemaName, schemaVersion] (Status&& status) {
            if (!status.is2xxOK()) {
                return RPCResponse(std::move(status), std::shared_ptr<dto::Schema>());
            }

            return getSchemaInternal(collectionName, schemaName, schemaVersion, false);
        });
    }

    if (sIt->second.size() > 0 && schemaVersion == ANY_VERSION) {
        std::shared_ptr<dto::Schema> foundSchema = sIt->second.begin()->second;
        return RPCResponse(Statuses::S200_OK("Found schema"), std::move(foundSchema));
    } else if (schemaVersion == ANY_VERSION && doCPORefresh) {
        return refreshSchemaCache(collectionName)
        .then([this, collectionName, schemaName, schemaVersion] (Status&& status) {
            if (!status.is2xxOK()) {
                return RPCResponse(std::move(status), std::shared_ptr<dto::Schema>());
            }

            return getSchemaInternal(collectionName, schemaName, schemaVersion, false);
        });
    } else if (schemaVersion == ANY_VERSION) {
        return RPCResponse(Statuses::S404_Not_Found("Could not find schema after CPO refresh"), std::shared_ptr<dto::Schema>());
    }

    auto vIt = sIt->second.find(schemaVersion);
    if (vIt == sIt->second.end() && !doCPORefresh) {
        return RPCResponse(Statuses::S404_Not_Found("Could not find schema after CPO refresh"), std::shared_ptr<dto::Schema>());
    } else if (vIt == sIt->second.end()) {
        return refreshSchemaCache(collectionName)
        .then([this, collectionName, schemaName, schemaVersion] (Status&& status) {
            if (!status.is2xxOK()) {
                return RPCResponse(std::move(status), std::shared_ptr<dto::Schema>());
            }

            return getSchemaInternal(collectionName, schemaName, schemaVersion, false);
        });
    }

    std::shared_ptr<dto::Schema> foundSchema = vIt->second;
    return RPCResponse(Statuses::S200_OK("Found schema"), std::move(foundSchema));
}

seastar::future<CreateQueryResult> K23SIClient::createQuery(const String& collectionName, const String& schemaName) {
    return getSchema(collectionName, schemaName, ANY_VERSION)
    .then([collectionName] (auto&& response) {
        if (!response.status.is2xxOK()) {
            return CreateQueryResult{std::move(response.status), Query()};
        }

        Query query;
        query.schema = response.schema;
        query.startScanRecord = SKVRecord(collectionName, query.schema);
        query.endScanRecord = SKVRecord(collectionName, query.schema);
        query.request.collectionName = collectionName;
        return CreateQueryResult{Statuses::S200_OK("Created query"), std::move(query)};
    });
}

// Called the first time a Query object is used to validate and setup the request object
void K2TxnHandle::_prepareQueryRequest(Query& query) {
    // Start and end records need to be at least a prefix of key fields. If they are a proper prefix,
    // then we need to pad either the start or end (depending on if it is a forward or reverse scan)
    // with null last in the unspecified fields so that the full prefix gets scanned by the query.
    // (Otherwise start and end may be equal for example which will scan no records)
    // We need to do it here in the client because we have semantic information on what fields are set
    // and because it can affect routing of the request.
    bool emptyField = false;
    for (String& key : query.startScanRecord.partitionKeys) {
        if (key == "") {
            emptyField = true;
            if (query.request.reverseDirection) {
                key = NullLastToKeyString();
            }
        } else if (emptyField) {
            throw K23SIClientException("Key fields of startScanRecord are not a prefix");
        }
    }
    for (String& key : query.startScanRecord.rangeKeys) {
        if (key == "") {
            emptyField = true;
            if (query.request.reverseDirection) {
                key = NullLastToKeyString();
            }
        } else if (emptyField) {
            throw K23SIClientException("Key fields of startScanRecord are not a prefix");
        }
    }

    emptyField = false;
    for (String& key : query.endScanRecord.partitionKeys) {
        if (key == "") {
            emptyField = true;
            if (!query.request.reverseDirection) {
                key = NullLastToKeyString();
            }
        } else if (emptyField) {
            throw K23SIClientException("Key fields of endScanRecord are not a prefix");
        }
    }
    for (String& key : query.endScanRecord.rangeKeys) {
        if (key == "") {
            emptyField = true;
            if (!query.request.reverseDirection) {
                key = NullLastToKeyString();
            }
        } else if (emptyField) {
            throw K23SIClientException("Key fields of endScanRecord are not a prefix");
        }
    }

    query.request.key = query.startScanRecord.getKey();
    query.request.endKey = query.endScanRecord.getKey();
    if (emptyField && !query.request.reverseDirection) {
        // If we've padded the end key for a forward prefix scan, we need to add one more byte
        // because the end key is exclusive but we want to include any record that may actually
        // have null last key fields set
        query.request.endKey.partitionKey.append(" ", 1);
        query.request.endKey.rangeKey.append(" ", 1);
    }


    if (query.request.key > query.request.endKey && !query.request.reverseDirection &&
                query.request.endKey.partitionKey != "") {
        throw K23SIClientException("Start key is greater than end key for forward direction query");
    } else if (query.request.key < query.request.endKey && query.request.reverseDirection &&
                query.request.key.partitionKey != "") {
        throw K23SIClientException("End key is greater than start key for reverse direction query");
    }

    query.request.mtr = _mtr;
    query.inprogress = true;
}

// Get one set of paginated results for a query. User may need to call again with same query
// object to get more results
seastar::future<QueryResult> K2TxnHandle::query(Query& query) {
    if (!_valid) {
        return seastar::make_exception_future<QueryResult>(K23SIClientException("Invalid use of K2TxnHandle"));
    }
    if (_failed) {
        return seastar::make_ready_future<QueryResult>(QueryResult(_failed_status));
    }

    if (query.done) {
        return seastar::make_exception_future<QueryResult>(K23SIClientException("Tried to use Query that is done"));
    }
    if (!query.inprogress) {
        _prepareQueryRequest(query);
    }

    _client->query_ops++;
    _ongoing_ops++;

    return _cpo_client->partitionRequest
        <dto::K23SIQueryRequest, dto::K23SIQueryResponse, dto::Verbs::K23SI_QUERY>
        (_options.deadline, query.request, query.request.reverseDirection, query.request.exclusiveKey)
    .then([this, &query] (auto&& response) {
        auto& [status, k2response] = response;
        _checkResponseStatus(status);
        _ongoing_ops--;

        if (!status.is2xxOK()) {
            query.done = true;
            return seastar::make_ready_future<QueryResult>(QueryResult(status));
        }

        if (k2response.nextToScan.partitionKey == "") {
            query.done = true;
        } else {
            query.request.key = std::move(k2response.nextToScan);
            query.request.exclusiveKey = std::move(k2response.exclusiveToken);
        }

        if (query.request.recordLimit >= 0) {
            query.request.recordLimit -= k2response.results.size();
            if (query.request.recordLimit == 0) {
                query.done = true;
            }
        }

        return QueryResult::makeQueryResult(_client, query, std::move(status), std::move(k2response));
    });
}

const dto::K23SI_MTR& K2TxnHandle::mtr() const {
    return _mtr;
}

} // namespace k2
