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
#include <deque>

#include <k2/common/Timer.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>

#include <boost/intrusive/list.hpp>
#include <seastar/core/shared_ptr.hh>

#include "Config.h"
#include "Log.h"
#include "Persistence.h"

namespace k2 {

namespace nsbi = boost::intrusive;

// A Transaction record
struct TxnRecord {
    dto::K23SI_MTR mtr;

    // the ranges which have to be finalized. These are delivered as part of the End request and we have to ensure
    // that the corresponding write intents are converted appropriately
    std::unordered_map<String, std::unordered_set<dto::KeyRangeVersion>> writeRanges;

    // The TRH key for this record. This is a routing key (think partition key) and we need this here
    // so that txn records can be correctly split/merged when a partition split/merge occurs
    dto::Key trh;

    // Expiry time point for retention window - these are driven off each TSO clock update
    dto::Timestamp rwExpiry;
    nsbi::list_member_hook<> rwLink;

    // for the heartbeat timer set - these are driven off local time
    TimePoint hbExpiry;
    nsbi::list_member_hook<> hbLink;

    // future used to track this transaction's background processing(e.g. finalize or delete)
    seastar::future<> bgTaskFut = seastar::make_ready_future();

    // flag which can be specified on txn options. If set, we wait for all participants to finalize their
    // write intents before responding to the client that their txn has been committed
    // This is useful for example in scenarios which write a lot of data before
    // running a benchmark. Without sync finalize, the benchmark may trigger too many PUSH conflicts
    // due to non-finalized WIs still being present in the system.
    bool syncFinalize = false;
    // Artificial wait interval used in testing to delay the time between transitioning from End to Finalize
    Duration timeToFinalize{0};

    // the transaction state
    dto::TxnRecordState state = dto::TxnRecordState::Created;

    // to tell what end action was used to finalize
    dto::EndAction finalizeAction = dto::EndAction::None;

    // if this transaction ever attempts to commit, we set this flag.
    bool hasAttemptedCommit{false};

    K2_PAYLOAD_FIELDS(mtr, writeRanges, trh, syncFinalize, state, finalizeAction, hasAttemptedCommit);
    K2_DEF_FMT(TxnRecord, mtr, writeRanges, trh, rwExpiry, hbExpiry, syncFinalize, timeToFinalize, state, finalizeAction, hasAttemptedCommit);

    // The last action on this TR (the action that put us into the above state)
    K2_DEF_ENUM_IC(Action,
        onCreate,
        onForceAbort,
        onRetentionWindowExpire,
        onHeartbeat,
        onHeartbeatExpire,
        onCommit,
        onAbort,
        onFinalizeComplete,
        onPersistSucceed,
        onPersistFail
    );

    typedef nsbi::list<TxnRecord, nsbi::member_hook<TxnRecord, nsbi::list_member_hook<>, &TxnRecord::rwLink>> RWList;
    typedef nsbi::list<TxnRecord, nsbi::member_hook<TxnRecord, nsbi::list_member_hook<>, &TxnRecord::hbLink>> HBList;

    // Records are linked in HB when created, and re-linked on each heartbeat from client.
    // we remove from HB when asked to END the transaction or when the transaction is force-aborted
    void unlinkHB(HBList& hblist);

    // Records are linked in RW when created
    // We remove from RW when asked to END, or when a force-abort reaches the end of retention window
    void unlinkRW(RWList& rwlist);
};  // class TxnRecord


// Manage K23SI transaction records.
class TxnManager {
public: // lifecycle
    TxnManager();
    ~TxnManager();

    // When started, we need to be told:
    // - the current retentionTimestamp
    // - the heartbeat interval for the collection
    seastar::future<> start(const String& collectionName, dto::Timestamp rts, Duration hbDeadline, std::shared_ptr<Persistence> persistence);

    // called when
    seastar::future<> gracefulStop();

    // called to update the retention timestamp when the server refreshes from TSO
    // We cache this value and use it to expire transactions when they are outside retention window.
    void updateRetentionTimestamp(dto::Timestamp rts);

    // returns the record for an id. Creates a new record in Created state if one does not exist
    TxnRecord& getTxnRecord(dto::K23SI_MTR&& mtr, dto::Key trhKey);

    // creates a new transaction. Returns 2xx code on success or other codes on failure
    seastar::future<Status> createTxn(dto::K23SI_MTR&& mtr, dto::Key incumbentTRHKey);

    // process a heartbeat for the given transaction. Returns 2xx code on success or other codes on failure
    seastar::future<Status> heartbeat(dto::K23SI_MTR&& mtr, dto::Key incumbentTRHKey);

    // executes a push of the given challenger against the given incumbent.
    seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
    push(dto::K23SI_MTR&& incumbentMTR, dto::K23SI_MTR&& challengerMTR, dto::Key incumbentTRHKey);

    // process a client's request to end a transaction
    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    endTxn(dto::K23SITxnEndRequest&& request);

    // inspect transactions
    seastar::future<std::tuple<Status, dto::K23SIInspectAllTxnsResponse>> inspectTxns();
    seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>> inspectTxn(dto::Timestamp txnTimestamp);

private:  // methods driving the state machine
    // delivers the given action for the given transaction and returns the status of executing the action
    // Returns the response from the execution of the newly entered state
    seastar::future<Status> _onAction(TxnRecord::Action action, TxnRecord& rec);

    // helper state handlers for each state we want to enter.
    // The contract here is that the state transition has been validated (i.e. the state transition is an allowed one)
    // but no other validation has been performed.
    // The response is one of the dto::K23SIStatus statuses. The action was successful iff result.is2xxOK()
    seastar::future<Status> _inProgress(TxnRecord& rec);
    seastar::future<Status> _forceAborted(TxnRecord& rec);
    seastar::future<Status> _commitPIP(TxnRecord& rec);
    seastar::future<Status> _abortPIP(TxnRecord& rec);
    seastar::future<Status> _commit(TxnRecord& rec);
    seastar::future<Status> _abort(TxnRecord& rec);
    seastar::future<Status> _finalizedPIP(TxnRecord& rec);

    // helper used to end (Commit/Abort) a transaction
    seastar::future<Status> _endPIPHelper(TxnRecord& rec);
    seastar::future<Status> _endHelper(TxnRecord& rec);

    // Helper method which finalizes a transaction
    seastar::future<Status> _finalizeTransaction(TxnRecord& rec, FastDeadline deadline);

    // helper handler for retries of endTxn requests
    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    _endTxnRetry(TxnRecord& rec, dto::K23SITxnEndRequest&& request);

    // add a function to the list of background tasks. The function will run when the current chain
    // of background tasks completes.
    template<typename Func>
    void _addBgTask(TxnRecord& rec, Func&& func);

    // this helper is used to generate additional finalization requests after
    // a pmap update is detected during finalization of a particular range
    // failedReq is the request on which we detected the pmap change
    // requests is the deque we'll append new requests into.
    void _genFinalizeReqsAfterPMAPUpdate(dto::K23SITxnFinalizeRequest&& failedReq,
        std::deque<std::tuple<dto::K23SITxnFinalizeRequest, dto::KeyRangeVersion>>& requests,
        dto::KeyRangeVersion&& krv);

private: // fields

    // Expiry lists. The order in the list is ascending so that the oldest item would be in the front
    TxnRecord::RWList _rwlist;
    TxnRecord::HBList _hblist;

    // heartbeats checks are driven off single timer.
    PeriodicTimer _hbTimer;

    // the primary store for transaction records
    std::unordered_map<dto::Timestamp, TxnRecord> _transactions;

    // the configuration for the k23si module
    K23SIConfig _config;

    // the collection-wide deadline for heartbeating of transactions
    Duration _hbDeadline;

    // this is the retention window timestamp we should use for new transactions
    dto::Timestamp _retentionTs;

    bool _stopping = false;

    String _collectionName;
    CPOClient _cpo;

    std::shared_ptr<Persistence> _persistence;
}; // class TxnManager

}  // namespace k2
