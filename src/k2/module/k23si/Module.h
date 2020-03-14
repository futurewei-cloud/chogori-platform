#pragma once

#include <map>
#include <unordered_map>
#include <deque>

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>

#include "ReadCache.h"
#include "TransactionRecord.h"
#include "DataRecord.h"

namespace k2 {

class K23SIPartitionModule {
public:
    K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition);
    ~K23SIPartitionModule();

    seastar::future<> start();
    seastar::future<> stop();
public:
    // verb handlers
    // Read is called when we either get a new read, or after we perform a push operation on behalf of an incoming
    // read (recursively). We only perform the recursive attempt to read if we won this PUSH operation.
    // If this is called after a push, sitMTR will be the mtr of the sitting(and now aborted) WI
    // compKey is the composite key we get from the dto Key
    seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
    handleRead(dto::K23SIReadRequest&& request, dto::K23SI_MTR sitMTR);

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    handleWrite(dto::K23SIWriteRequest<Payload>&& request);

    seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
    handleTxnPush(dto::K23SITxnPushRequest&& request);

    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    handleTxnEnd(dto::K23SITxnEndRequest&& request);

private:
    // this method executes a push operation at the given TRH in order to
    // select a winner between the sitting transaction's mtr (sitMTR)
    // and the incoming transaction which want's to push (pushMTR)
    // It returns the winner MTR.
    // The losing transaction must always abort. In cases where the losing txn is the sitting one,
    // we will abort its state at the TRH.
    // In cases where the pusing txn is to be aborted, whoever calls _doPush() has to signal
    // the client that they must issue an onEnd(Abort).
    seastar::future<dto::K23SI_MTR>
    _doPush(dto::Key trh, dto::K23SI_MTR sitMTR, dto::K23SI_MTR pushMTR);

    // helper method to convert a DataRecord into a read response with appropriate code
    seastar::future<std::tuple<Status, dto::K23SIReadResponse<Payload>>>
    _makeReadResponse(DataRecord* rec) const;

    // helper method used to clean up WI which have been removed
    void _queueWICleanup(DataRecord rec);

private:
    dto::CollectionMetadata _cmeta;
    dto::Partition _partition;
    // to store data. The deque contains versions of a key, sorted in decreasing order of their ts.end.
    // (newest item is at front of the deque)
    // Duplicates are not allowed
    std::map<dto::Key, std::deque<DataRecord>> _indexer;
    // to store transactions
    std::unordered_map<dto::Key, TransactionRecord> _transactions;
    // read cache for keeping track of latest reads
    std::unique_ptr<ReadCache<dto::Key, dto::Timestamp>> _readCache;

    // the timestamp of the end of the retention window. We do not allow operations to occur before this timestamp
    dto::Timestamp _retentionTimestamp;

    // the minimum retention window we can support.
    // Governs how often we refresh the retention timestamp and also minimum limit on how long transactions can run
    ConfigDuration _minimumRetentionPeriod{"retention_minimum", 1h};

    // timer used to refresh the retention timestamp from the TSO
    seastar::timer<> _retentionUpdateTimer;

    // used to tell if there is a refresh in progress so that we don't stop() too early
    seastar::future<> _retentionRefresh = seastar::make_ready_future();

    // validate requests are coming to the correct partition
    template<typename RequestT>
    bool _validateRequestPartition(const RequestT& r) const {
        return r.collectionName == _cmeta.name && r.pvid == _partition.pvid;
    }

    template <typename RequestT>
    bool _validateRetentionWindow(const RequestT& r) const {
        return r.mtr.timestamp.compareCertain(_retentionTimestamp) >= 0;
    }
};

} // ns k2
