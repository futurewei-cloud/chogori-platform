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
    K2DEBUG("ctor, mtr=" << _mtr);
}

void K2TxnHandle::checkResponseStatus(Status& status) {
    if (status == dto::K23SIStatus::AbortConflict ||
        status == dto::K23SIStatus::AbortRequestTooOld ||
        status == dto::K23SIStatus::OperationNotAllowed) {
        _failed = true;
        _failed_status = status;
    }

    if (status == dto::K23SIStatus::AbortConflict) {
        _client->abort_conflicts++;
    }

    if (status == dto::K23SIStatus::AbortRequestTooOld) {
        _client->abort_too_old++;
        K2DEBUG("Abort: txn too old: " << _mtr);
    }
}

void K2TxnHandle::makeHeartbeatTimer() {
    K2DEBUG("makehb, mtr=" << _mtr);
    _heartbeat_timer.setCallback([this] {
        _client->heartbeats++;

        auto* request = new dto::K23SITxnHeartbeatRequest {
            dto::Partition::PVID(), // Will be filled in by PartitionRequest
            _trh_collection,
            _trh_key,
            _mtr
        };

        K2DEBUG("send hb for " << _mtr);

        return _cpo_client->PartitionRequest<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse, dto::Verbs::K23SI_TXN_HEARTBEAT>(Deadline(_heartbeat_interval), *request)
        .then([this] (auto&& response) {
            auto& [status, k2response] = response;
            checkResponseStatus(status);
            if (_failed) {
                K2DEBUG("txn failed: cancelling hb in " << _mtr);
                _heartbeat_timer.cancel();
            }
        }).finally([request, this] {
            delete request;
        });
    });
}

dto::K23SIReadRequest* K2TxnHandle::makeReadRequest(const dto::SKVRecord& record) const {
    for (const String& key : record.partitionKeys) {
        if (key == "") {
            throw new std::runtime_error("Partition key field not set for read request");
        }
    }
    for (const String& key : record.rangeKeys) {
        if (key == "") {
            throw new std::runtime_error("Range key field not set for read request");
        }
    }

    return new dto::K23SIReadRequest{
        dto::Partition::PVID(), // Will be filled in by PartitionRequest
        record.collectionName,
        _mtr,
        record.getKey()
    };
}

dto::K23SIWriteRequest* K2TxnHandle::makeWriteRequest(dto::SKVRecord& record, bool erase) {
    for (const String& key : record.partitionKeys) {
        if (key == "") {
            throw new std::runtime_error("Partition key field not set for write request");
        }
    }
    for (const String& key : record.rangeKeys) {
        if (key == "") {
            throw new std::runtime_error("Range key field not set for read request");
        }
    }

    dto::Key key = record.getKey();

    if (!_write_set.size()) {
        _trh_key = key;
        _trh_collection = record.collectionName;
    }
    _write_set.push_back(key);

    return new dto::K23SIWriteRequest{
        dto::Partition::PVID(), // Will be filled in by PartitionRequest
        record.collectionName,
        _mtr,
        _trh_key,
        erase,
        _write_set.size() == 1,
        key,
        record.storage.share()
    };
}

seastar::future<EndResult> K2TxnHandle::end(bool shouldCommit) {
    if (!_valid) {
        return seastar::make_exception_future<EndResult>(std::runtime_error("Tried to end() an invalid TxnHandle"));
    }    
    // User is not allowed to call anything else on this TxnHandle after end()
    _valid = false;

    if (_ongoing_ops != 0) {
        return seastar::make_exception_future<EndResult>(std::runtime_error("Tried to end() with ongoing ops"));
    }

    if (!_write_set.size()) {
        _client->successful_txns++;

        if (_failed && shouldCommit) {
            // This means a bug in the application because there is no heartbeat for a RO txn,
            // so the app should have seen the failure on a read and aborted
            K2WARN("Tried to commit a failed RO transaction, mtr: " << _mtr);
            return seastar::make_ready_future<EndResult>(EndResult(_failed_status));
        }

        return seastar::make_ready_future<EndResult>(EndResult(Statuses::S200_OK("default end result")));
    }

    auto* request  = new dto::K23SITxnEndRequest {
        dto::Partition::PVID(), // Will be filled in by PartitionRequest
        _trh_collection,
        _trh_key,
        _mtr,
        shouldCommit && !_failed ? dto::EndAction::Commit : dto::EndAction::Abort,
        std::move(_write_set),
        _options.syncFinalize
    };

    K2DEBUG("Cancel hb for " << _mtr);
    _heartbeat_timer.cancel();

    return _cpo_client->PartitionRequest
        <dto::K23SITxnEndRequest, dto::K23SITxnEndResponse, dto::Verbs::K23SI_TXN_END>
        (Deadline<>(_txn_end_deadline), *request).
        then([this, shouldCommit] (auto&& response) {
            auto& [status, k2response] = response;
            if (status.is2xxOK() && !_failed) {
                _client->successful_txns++;
            } else if (!status.is2xxOK()){
                K2WARN("TxnEndRequest failed: " << status << " mtr: " << _mtr);
            }

            if (_failed && shouldCommit && status.is2xxOK()) {
                // This could either be a bug in the application, where the failure was a read/write op
                // and the app should know to abort, or it is a normal scenario where there was a
                // failure on the heartbeat and the app was not aware.
                //
                // Either way, we aborted the transaction but we need to indicate to the app that
                // it was not commited
                K2DEBUG("Tried to commit a failed transaction, _mtr: " << _mtr);
                status = _failed_status;
            }

            return _heartbeat_timer.stop().then([this, s=std::move(status)] () {
                // TODO get min transaction time from TSO client
                auto time_spent = Clock::now() - _start_time;
                if (time_spent < 10us) {
                    auto sleep = 10us - time_spent;
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
        _tsoClient(AppBase().getDist<TSO_ClientLib>().local()), _gen(std::random_device()()) {
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
    K2INFO("_cpo: " << _cpo());
    cpo_client = CPOClient(String(_cpo()));

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

    return cpo_client.CreateAndWaitForCollection(Deadline<>(create_collection_deadline()), std::move(metadata), std::move(endpoints), std::move(rangeEnds));
}

seastar::future<K2TxnHandle> K23SIClient::beginTxn(const K2TxnOptions& options) {
    auto start_time = Clock::now();
    return _tsoClient.GetTimestampFromTSO(start_time)
    .then([this, start_time, options] (auto&& timestamp) {
        dto::K23SI_MTR mtr{
            _rnd(_gen),
            std::move(timestamp),
            options.priority
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
            K2INFO("Failed to refresh schemas from CPO: " << status);
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
void K2TxnHandle::prepareQueryRequest(Query& query) {
    bool emptyField = false;
    for (const String& key : query.startScanRecord.partitionKeys) {
        if (key == "") {
            emptyField = true;
        } else if (emptyField) {
            throw new std::runtime_error("Key fields of startScanRecord are not a prefix");
        }
    }
    for (const String& key : query.startScanRecord.rangeKeys) {
        if (key == "") {
            emptyField = true;
        } else if (emptyField) {
            throw new std::runtime_error("Key fields of startScanRecord are not a prefix");
        }
    }

    emptyField = false;
    for (const String& key : query.endScanRecord.partitionKeys) {
        if (key == "") {
            emptyField = true;
        } else if (emptyField) {
            throw new std::runtime_error("Key fields of endScanRecord are not a prefix");
        }
    }
    for (const String& key : query.endScanRecord.rangeKeys) {
        if (key == "") {
            emptyField = true;
        } else if (emptyField) {
            throw new std::runtime_error("Key fields of endScanRecord are not a prefix");
        }
    }

    query.request.key = query.startScanRecord.getKey();
    query.request.endKey = query.endScanRecord.getKey();
    if (query.request.key > query.request.endKey && !query.request.reverseDirection) {
        throw new std::runtime_error("Start key is greater than end key for forward direction query");
    } else if (query.request.key < query.request.endKey && query.request.reverseDirection) {
        throw new std::runtime_error("End key is greater than start key for reverse direction query");
    }

    query.request.mtr = _mtr;
    query.inprogress = true;
}

// Get one set of paginated results for a query. User may need to call again with same query
// object to get more results
seastar::future<QueryResult> K2TxnHandle::query(Query& query) {
    if (!_valid) {
        return seastar::make_exception_future<QueryResult>(std::runtime_error("Invalid use of K2TxnHandle"));
    }
    if (_failed) {
        return seastar::make_ready_future<QueryResult>(QueryResult(_failed_status));
    }

    if (query.done) {
        return seastar::make_exception_future<QueryResult>(std::runtime_error("Tried to use Query that is done"));
    }
    if (!query.inprogress) {
        prepareQueryRequest(query);
    }

    _client->query_ops++;
    _ongoing_ops++;

    return _cpo_client->PartitionRequest
        <dto::K23SIQueryRequest, dto::K23SIQueryResponse, dto::Verbs::K23SI_QUERY>
        (_options.deadline, query.request)
    .then([this, &query] (auto&& response) {
        auto& [status, k2response] = response;
        checkResponseStatus(status);
        _ongoing_ops--;

        if (!status.is2xxOK()) {
            query.done = true;
            return seastar::make_ready_future<QueryResult>(QueryResult(status));
        }

        if (k2response.nextToScan.partitionKey == "") {
            query.done = true;
        } else {
            query.request.key = std::move(k2response.nextToScan);
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
