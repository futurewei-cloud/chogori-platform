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
#include <k2/tso/client_lib/tso_clientlib.h>

#include "ReadCache.h"
#include "TxnManager.h"
#include "Config.h"
#include "Persistence.h"

namespace k2 {

class K23SIPartitionModule {
public: // lifecycle
    K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition);
    ~K23SIPartitionModule();

    seastar::future<> start();
    seastar::future<> gracefulStop();

   public:
    // verb handlers
    // Read is called when we either get a new read, or after we perform a push operation on behalf of an incoming
    // read (recursively). We only perform the recursive attempt to read if we won this PUSH operation.
    // If this is called after a push, sitMTR will be the mtr of the sitting(and now aborted) WI
    // compKey is the composite key we get from the dto Key
    seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
    handleRead(dto::K23SIReadRequest&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline);

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    handleWrite(dto::K23SIWriteRequest<Payload>&& request, dto::K23SI_MTR sitMTR, FastDeadline deadline);

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
    // this method executes a push operation at the given TRH in order to
    // select a winner between the sitting transaction's mtr (sitMTR)
    // and the incoming transaction which want's to push (pushMTR)
    // It returns the winner MTR.
    // The losing transaction must always abort. In cases where the losing txn is the sitting one,
    // we will abort its state at the TRH.
    // In cases where the pusing txn is to be aborted, whoever calls _doPush() has to signal
    // the client that they must issue an onEnd(Abort).
    seastar::future<dto::K23SI_MTR>
    _doPush(String collectionName, dto::TxnId sitTxnId, dto::K23SI_MTR pushMTR, FastDeadline deadline);

    // helper method used to clean up WI which have been removed
    void _queueWICleanup(dto::DataRecord&& rec);

    // validate requests are coming to the correct partition. return true if request is valid
    template<typename RequestT>
    bool _validateRequestPartition(const RequestT& req) const {
        auto result = req.collectionName == _cmeta.name && req.pvid == _partition().pvid && _partition.owns(req.key);
        K2DEBUG("Partition: " << _partition << ", partition validation " << (result? "passed": "failed")
                << ", for request=" << req);
        return result;
    }

    // validate requests are within the retention window for the collection. return true if request is valid
    template <typename RequestT>
    bool _validateRetentionWindow(const RequestT& req) const {
        auto result = req.mtr.timestamp.compareCertain(_retentionTimestamp) >= 0;
        K2DEBUG("Partition: " << _partition << ", retention validation " << (result ? "passed" : "failed") << ", have=" << _retentionTimestamp << ", for request=" << req);
        return result;
    }

    // validate writes are not stale - older than the newest committed write or past a recent read.
    // return true if request is valid
    bool _validateStaleWrite(dto::K23SIWriteRequest<Payload>& request, std::deque<dto::DataRecord>& versions);

    // helper method used to create and persist a WriteIntent
    seastar::future<> _createWI(dto::K23SIWriteRequest<Payload>&& request, std::deque<dto::DataRecord>& versions, FastDeadline deadline);

    // recover data upon startup
    seastar::future<> _recovery();

private: // members
    // the metadata of our collection
    dto::CollectionMetadata _cmeta;

    // the partition we're assigned
    dto::OwnerPartition _partition;

    // to store data. The deque contains versions of a key, sorted in decreasing order of their ts.end.
    // (newest item is at front of the deque)
    // Duplicates are not allowed
    std::map<dto::Key, std::deque<dto::DataRecord>> _indexer;

    // to store transactions
    TxnManager _txnMgr;

    // read cache for keeping track of latest reads
    std::unique_ptr<ReadCache<dto::Key, dto::Timestamp>> _readCache;

    // schema name -> (schema version -> schema)
    std::unordered_map<String, std::unordered_map<uint32_t, dto::Schema>> _schemas;

    // config
    K23SIConfig _config;

    // the timestamp of the end of the retention window. We do not allow operations to occur before this timestamp
    dto::Timestamp _retentionTimestamp;

    // timer used to refresh the retention timestamp from the TSO
    seastar::timer<> _retentionUpdateTimer;

    // used to tell if there is a refresh in progress so that we don't stop() too early
    seastar::future<> _retentionRefresh = seastar::make_ready_future();

    // TODO persistence
    Persistence _persistence;

    CPOClient _cpo;

    // get timeNow Timestamp from TSO
    seastar::future<dto::Timestamp> getTimeNow() {
        thread_local TSO_ClientLib& tsoClient = AppBase().getDist<TSO_ClientLib>().local();
        return tsoClient.GetTimestampFromTSO(Clock::now());
    }
};

} // ns k2
