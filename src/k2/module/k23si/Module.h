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

#pragma once

#include <k2/appbase/AppEssentials.h>
#include <k2/common/Chrono.h>
#include <k2/cpo/client/Client.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>
#include <k2/tso/client/Client.h>

#include "Config.h"
#include "Indexer.h"
#include "Log.h"
#include "Persistence.h"
#include "TxnManager.h"
#include "TxnWIMetaManager.h"

namespace k2 {

class K23SIPartitionModule {
public: // lifecycle
    K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition, String cpoEndpoint);
    ~K23SIPartitionModule();

    seastar::future<> start();
    seastar::future<> gracefulStop();

    // recover data upon startup
    seastar::future<> _recovery();

public:
    // verb handlers
    // Read is called when we either get a new read, or after we perform a push operation
    // on behalf of an incoming read (recursively). We only perform the recursive attempt
    // to read if we were allowed to retry by the PUSH operation
    seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
    handleRead(dto::K23SIReadRequest&& request, FastDeadline deadline, uint32_t count);

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    handleWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline);

    seastar::future<std::tuple<Status, dto::K23SIQueryResponse>>
    handleQuery(dto::K23SIQueryRequest&& request, dto::K23SIQueryResponse&& response, FastDeadline deadline, uint32_t count);

    seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
    handleTxnPush(dto::K23SITxnPushRequest&& request);

    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    handleTxnEnd(dto::K23SITxnEndRequest&& request);

    seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
    handleTxnHeartbeat(dto::K23SITxnHeartbeatRequest&& request);

    seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
    handleTxnFinalize(dto::K23SITxnFinalizeRequest&& request);

    seastar::future<std::tuple<Status, dto::K23SIPushSchemaResponse>>
    handlePushSchema(dto::K23SIPushSchemaRequest&& request);

    // For test and debug purposes, not normal transaction processsing
    seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
    handleInspectRecords(dto::K23SIInspectRecordsRequest&& request);

    // For test and debug purposes, not normal transaction processsing
    seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
    handleInspectTxn(dto::K23SIInspectTxnRequest&& request);

    // For test and debug purposes, not normal transaction processsing
    seastar::future<std::tuple<Status, dto::K23SIInspectWIsResponse>>
    handleInspectWIs(dto::K23SIInspectWIsRequest&& request);

    // For test and debug purposes, not normal transaction processsing
    seastar::future<std::tuple<Status, dto::K23SIInspectAllTxnsResponse>>
    handleInspectAllTxns(dto::K23SIInspectAllTxnsRequest&& request);

    // For test and debug purposes, not normal transaction processsing
    seastar::future<std::tuple<Status, dto::K23SIInspectAllKeysResponse>>
    handleInspectAllKeys(dto::K23SIInspectAllKeysRequest&& request);

private: // methods
    // this method executes a push operation at the TRH for the given incumbent in order to
    // determine if the challengerMTR should be allowed to proceed.
    // It returns 2xxOK iff the challenger should be allowed to proceed. If not allowed, the client
    // who issued the request must be notified to abort their transaction.
    // This method also has the side-effect of handling the cleanup of the WI which triggered
    // the push operation.
    // In cases where this push operation caused the incumbent transaction to be aborted, the
    // incumbent transaction state at the TRH will be updated to reflect the abort decision.
    // The incumbent transaction will discover upon commit that the txn has been aborted.
    seastar::future<Status>
    _doPush(dto::Key key, dto::Timestamp incumbentId, dto::K23SI_MTR challengerMTR, FastDeadline deadline, uint32_t count);

    // validate requests are coming to the correct partition. return true if request is valid
    template<typename RequestT>
    bool _validateRequestPartition(const RequestT& req) const;

    // return true iff the given timestamp is within the retention window for the collection.
    bool _validateRetentionWindow(const dto::Timestamp& ts) const;

    // validate keys in the requests must include non-empty partitionKey. return true if request parameter is valid
    template <typename RequestT>
    bool _validateRequestPartitionKey(const RequestT& req) const;

    // validate writes are not stale - older than the newest committed write or past a recent read.
    // return true if request is valid
    Status _validateStaleWrite(const dto::K23SIWriteRequest& request, const Indexer::Iterator& iter);

    // validate an incoming write request
    Status _validateWriteRequest(const dto::K23SIWriteRequest& request);

    template <class RequestT>
    Status _validateReadRequest(const RequestT& request) const;

    // helper method used to create and persist a WriteIntent
    Status _createWI(dto::K23SIWriteRequest&& request, Indexer::Iterator& iter);

    // helper method used to make a projection SKVRecord payload
    bool _makeProjection(dto::SKVRecord::Storage& fullRec, dto::K23SIQueryRequest& request, dto::SKVRecord::Storage& projectionRec);

    // method to parse the partial record to full record, return turn if parse successful
    bool _parsePartialRecord(dto::K23SIWriteRequest& request, dto::DataRecord& previous);

    // make every fields for a partial update request in the condition of same schema and same version
    bool _makeFieldsForSameVersion(dto::Schema& schema, dto::K23SIWriteRequest& request, dto::DataRecord& version);

    // make every fields for a partial update request in the condition of same schema and different versions
    bool _makeFieldsForDiffVersion(dto::Schema& schema, dto::Schema& baseSchema, dto::K23SIWriteRequest& request, dto::DataRecord& version);

    // find field number matches to 'fieldName' and 'fieldtype' in schema, return -1 if do not find
    std::size_t _findField(const dto::Schema schema, k2::String fieldName ,dto::FieldType fieldtype);

    // judge whether fieldIdx is in fieldsForPartialUpdate. return true if yes(is in fieldsForPartialUpdate).
    bool _isUpdatedField(uint32_t fieldIdx, std::vector<uint32_t> fieldsForPartialUpdate);

    // Helper for iterating over the indexer. Advances to the next iterator position and registers an observation
    void _scanAdvance(Indexer::Iterator& iter, const dto::K23SIQueryRequest& request);

    // Helper for handleQuery. Returns a directional Iterator to start the scan at, accounting for
    // desired schema and reverse direction scan
    Indexer::Iterator _initializeScan(const dto::K23SIQueryRequest& request);

    // Helper for handleQuery. Checks to see if the indexer scan should stop.
    bool _isScanDone(const Indexer::Iterator& iter, const dto::K23SIQueryRequest& request, size_t response_size, uint64_t num_scans);

    // Helper for handleQuery. Returns continuation token (aka response.nextToScan)
    dto::Key _getContinuationToken(const Indexer::Iterator& iter, const dto::K23SIQueryRequest& request,
                                            dto::K23SIQueryResponse& response, size_t response_size);

    std::tuple<Status, bool> _doQueryFilter(dto::K23SIQueryRequest& request, dto::SKVRecord::Storage& storage);

    seastar::future<> _registerVerbs();

    // Helper method which generates an RPCResponce chained after a successful persistence flush
    template <typename ResponseT>
    seastar::future<std::tuple<Status, ResponseT>> _respondAfterFlush(std::tuple<Status, ResponseT>&& tuple);

    // helper used to process the designate TRH part of a write request
    seastar::future<Status> _designateTRH(dto::K23SI_MTR mtr, dto::Key trhKey);

    // helper used to process the write part of a write request
    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    _processWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline, uint32_t count);

    void _unregisterVerbs();

    // helper used to finalize all local WIs for a give transaction
    Status _finalizeTxnWIs(dto::Timestamp txnts, dto::EndAction action);

    void _registerMetrics();

private:  // members
    // to get K2 timestamps
    tso::TSOClient& _tsoClient;

    // the metadata of our collection
    dto::CollectionMetadata _cmeta;

    // the partition we're assigned
    dto::OwnerPartition _partition;

    // the data indexer
    Indexer _indexer;

    // manage transaction records as a coordinator
    TxnManager _txnMgr;

    // manage write intent metadata records as a participant
    TxnWIMetaManager _twimMgr;

    // schema name -> (schema version -> schema)
    std::unordered_map<String, std::unordered_map<uint32_t, std::shared_ptr<dto::Schema>>> _schemas;

    // config
    K23SIConfig _config;

    // the timestamp of the end of the retention window. We do not allow operations to occur before this timestamp
    dto::Timestamp _retentionTimestamp;

    // timer used to refresh the retention timestamp from the TSO
    PeriodicTimer _retentionUpdateTimer;

    std::shared_ptr<Persistence> _persistence;

    cpo::CPOClient _cpo;
    String _cpoEndpoint; // Obtained from the CPO assignment request

    sm::metric_groups _metricGroups;

    //metrics
    uint64_t _totalWI{0}; // for number of active WIs
    uint64_t _recordVersions{0};
    uint64_t _totalCommittedPayload{0}; //total committed user payload size
    uint64_t _finalizedWI{0}; // total number of finalized WI

    k2::ExponentialHistogram _readLatency;
    k2::ExponentialHistogram _writeLatency;
    k2::ExponentialHistogram _queryPageLatency;
    k2::ExponentialHistogram _pushLatency;
    k2::ExponentialHistogram _queryPageScans;
    k2::ExponentialHistogram _queryPageReturns;
};

    }  // ns k2
