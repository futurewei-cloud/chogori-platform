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

#include <map>
#include <unordered_map>
#include <deque>

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>
#include <k2/common/Chrono.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/tso/client/tso_clientlib.h>

#include "ReadCache.h"
#include "TxnManager.h"
#include "TxnWIMetaManager.h"
#include "Config.h"
#include "Persistence.h"
#include "Log.h"

namespace k2 {


// the type holding multiple committed versions of a key
typedef std::deque<dto::DataRecord> VersionsT;

struct VersionSet {
    // If there is a WI, that means that we also have a TxnWIMeta with the twimMgr.
    // The invariant we maintain is
    // if there is a WI set, then: the local twim's state is in InProgress (use isInProgress() to check)
    // The implementation logic then just has to look for the presense of this WI to determine if a push is needed.
    // After a PUSH operation, if we determine that the incumbent should be finalized(committed/aborted), we
    // just take care of the WI which triggered the PUSH. This is needed to make room for a new WI in cases of
    // Write-Write PUSH. We still rely on the TRH to take care of the full finalization for the rest of the WIs.
    //
    // For optimization purposes, if we determine that the incumbent should be finalized, we update its state.
    // Future PUSH operations for this txn (from other WIs on this node) will be determined locally.
    // This allows us to
    // - perform finalization in a rate-limited fashion (i.e. have only some WIs finalized for a txn)
    // - finalize out WIs which actively trigger a conflict, without requiring finalization for the entire txn.
    std::optional<WriteIntent> WI;
    VersionsT committed;
    bool empty() const {
        return !WI.has_value() && committed.empty();
    }
};

// the type holding versions for all keys, i.e. the indexer
typedef std::map<dto::Key, VersionSet> IndexerT;
typedef IndexerT::iterator IndexerIterator;

class K23SIPartitionModule {
public: // lifecycle
    K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition);
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
    handleRead(dto::K23SIReadRequest&& request, FastDeadline deadline);

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    handleWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline);

    seastar::future<std::tuple<Status, dto::K23SIQueryResponse>>
    handleQuery(dto::K23SIQueryRequest&& request, dto::K23SIQueryResponse&& response, FastDeadline deadline);

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
    _doPush(dto::Key key, dto::Timestamp incumbentId, dto::K23SI_MTR challengerMTR, FastDeadline deadline);

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
    template <typename RequestT>
    Status _validateStaleWrite(const RequestT& req, const VersionSet& versions);

    // validate an incoming write request
    Status _validateWriteRequest(const dto::K23SIWriteRequest& request, const VersionSet& versions);

    template <class RequestT>
    Status _validateReadRequest(const RequestT& request) const;

    // helper method used to create and persist a WriteIntent
    Status _createWI(dto::K23SIWriteRequest&& request, VersionSet& versions);

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

    // Helper for iterating over the indexer, modifies it to end() if iterator would go past the target schema
    // or if it would go past begin() for reverse scan. Starting iterator must not be end() and must
    // point to a record with the target schema
    void _scanAdvance(IndexerIterator& it, bool reverseDirection, const String& schema);

    // Helper for handleQuery. Returns an iterator to start the scan at, accounting for
    // desired schema and (eventually) reverse direction scan
    IndexerIterator _initializeScan(const dto::Key& start, bool reverse, bool exclusiveKey);

    // Helper for handleQuery. Checks to see if the indexer scan should stop.
    bool _isScanDone(const IndexerIterator& it, const dto::K23SIQueryRequest& request, size_t response_size);

    // Helper for handleQuery. Returns continuation token (aka response.nextToScan)
    dto::Key _getContinuationToken(const IndexerIterator& it, const dto::K23SIQueryRequest& request,
                                            dto::K23SIQueryResponse& response, size_t response_size);

    std::tuple<Status, bool> _doQueryFilter(dto::K23SIQueryRequest& request, dto::SKVRecord::Storage& storage);

    // the the data record in the version set which is not newer than the given timestsamp
    // The returned pointer is invalid if any modifications are made to the indexer. Will also
    // return the current WI if it matches exactly the given timestamp. In other words, it
    // returns a record that is valid to return for to a read request for the given timestamp.
    dto::DataRecord* _getDataRecordForRead(VersionSet& versions, dto::Timestamp& timestamp);

    // For a given challenger timestamp and key, check if a push is needed against a WI
    bool _checkPushForRead(const VersionSet& versions, const dto::Timestamp& timestamp);

    // Helper to remove a WI and delete the key from the indexer of there are no committed records
    void _removeWI(IndexerIterator it);

    // get timeNow Timestamp from TSO
    seastar::future<dto::Timestamp> getTimeNow() {
        TSO_ClientLib& tsoClient = AppBase().getDist<TSO_ClientLib>().local();
        return tsoClient.getTimestampFromTSO(Clock::now());
    }

    seastar::future<> _registerVerbs();

    // Helper method which generates an RPCResponce chained after a successful persistence flush
    template <typename ResponseT>
    seastar::future<std::tuple<Status, ResponseT>> _respondAfterFlush(std::tuple<Status, ResponseT>&& tuple);

    // helper used to process the designate TRH part of a write request
    seastar::future<Status> _designateTRH(dto::K23SI_MTR mtr, dto::Key trhKey);

    // helper used to process the write part of a write request
    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    _processWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline);

    void _unregisterVerbs();

    // helper used to finalize all local WIs for a give transaction
    Status _finalizeTxnWIs(dto::Timestamp txnts, dto::EndAction action);

private:  // members
    // the metadata of our collection
    dto::CollectionMetadata _cmeta;

    // the partition we're assigned
    dto::OwnerPartition _partition;

    // to store data. The deque contains versions of a key, sorted in decreasing order of their ts.end.
    // (newest item is at front of the deque)
    // Duplicates are not allowed
    IndexerT _indexer;

    // manage transaction records as a coordinator
    TxnManager _txnMgr;

    // manage write intent metadata records as a participant
    TxnWIMetaManager _twimMgr;

    // read cache for keeping track of latest reads
    std::unique_ptr<ReadCache<dto::Key, dto::Timestamp>> _readCache;

    // schema name -> (schema version -> schema)
    std::unordered_map<String, std::unordered_map<uint32_t, std::shared_ptr<dto::Schema>>> _schemas;

    // config
    K23SIConfig _config;

    // the timestamp of the end of the retention window. We do not allow operations to occur before this timestamp
    dto::Timestamp _retentionTimestamp;

    // the start time for this partition.
    dto::Timestamp _startTs;

    // timer used to refresh the retention timestamp from the TSO
    PeriodicTimer _retentionUpdateTimer;

    std::shared_ptr<Persistence> _persistence;

    CPOClient _cpo;
};

} // ns k2
