/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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
#include <k2/cpo/client/Client.h>
#include <k2/dto/K23SI.h>

#include <unordered_set>
#include <vector>

#include "Config.h"
#include "Persistence.h"

namespace k2 {
namespace nsbi = boost::intrusive;

//- keeps the list of keys to finalize at a particular transaction participant
//- holds a reference to the TxnRecord (via the trh and mtr) used for PUSH operations
// The invariants we maintain are:
// * If there is a WI for a given key in the indexer
//      - we also have a TxnWIMeta with the twimMgr
//      - the local twim's state is in InProgress (use isInProgress() to check)
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
struct TxnWIMeta {
    dto::Key trh;
    String trhCollection;
    dto::K23SI_MTR mtr;
    std::unordered_set<dto::Key> writeKeys;
    dto::EndAction finalizeAction =dto::EndAction::None;
    dto::TxnWIMetaState state = dto::TxnWIMetaState::Created;
    nsbi::list_member_hook<> rwLink;

    bool isCommitted();
    bool isAborted();
    bool isInProgress();
    K2_DEF_FMT(TxnWIMeta, trh, trhCollection, mtr, writeKeys, finalizeAction, state);
    K2_PAYLOAD_FIELDS(trh, trhCollection, mtr, writeKeys, finalizeAction, state);

    typedef nsbi::list<TxnWIMeta, nsbi::member_hook<TxnWIMeta, nsbi::list_member_hook<>, &TxnWIMeta::rwLink>> RWList;

    // Records are linked in RW when created
    // We remove from RW when asked to END, or when a force-abort reaches the end of retention window
    void unlinkRW(RWList& rwlist);

    // future used to track this twim's background processing(e.g. finalize or delete)
    seastar::future<> bgTaskFut = seastar::make_ready_future();
}; // struct TxnWIMeta

class TxnWIMetaManager {
public:
    TxnWIMetaManager();
    ~TxnWIMetaManager();

    // Returns the TxnWIMeta for the given id, or null if no such record exists
    TxnWIMeta* getTxnWIMeta(dto::Timestamp ts);

    // Provide access to all twims in this manager
    const std::unordered_map<dto::Timestamp, TxnWIMeta>& twims() const;

    // When started, we need to be told:
    // - the current retentionTimestamp
    // - the heartbeat interval for the collection
    seastar::future<> start(dto::Timestamp rts, std::shared_ptr<Persistence> persistence, const String& cpoEndpoint);
    seastar::future<> gracefulStop();

    // Update the stored retention window timestamp.
    void updateRetentionTimestamp(dto::Timestamp rts);

    // Add the given write to the twim identified by the given mtr.
    // If the twim does not exist, a new one is created with the given trh key+collection
    Status addWrite(dto::K23SI_MTR&& mtr, dto::Key&& key, dto::Key&& trh, String&& trhCollection);

    // Set the local txn state to abort/commit and stop tracking the given key.
    // Used to perform local optimizations after PUSH operations
    Status abortWrite(dto::Timestamp txnId, dto::Key key);
    Status commitWrite(dto::Timestamp txnId, dto::Key key);

    // Set the state to commit/abort for the given txn
    Status endTxn(dto::Timestamp txnId, dto::EndAction action);

    // Set the state to finalizingWIs
    Status finalizingWIs(dto::Timestamp txnId);

    // Set the state to finalized
    Status finalizedTxn(dto::Timestamp txnId);

private:
    // timer to check for retention window expiry
    PeriodicTimer _rwTimer;

    // State machine actions
    K2_DEF_ENUM_IC(Action,
        onCreate,
        onCommit,
        onAbort,
        onFinalize,
        onFinalized,
        onRetentionWindowExpire,
        onPersistSucceed,
        onPersistFail
    );

    // The TWIMs we're managing, keyed by transaction timestamp
    std::unordered_map<dto::Timestamp, TxnWIMeta> _twims;

    // the persistence for the system
    std::shared_ptr<Persistence> _persistence;

    // the configuration for the k23si module
    K23SIConfig _config;

    // this is the retention window timestamp we should use for new transactions
    dto::Timestamp _retentionTs;

    // flag set upon shutdown
    bool _stopping = false;

    // keep track of records we need to handle at Retention Window Expiry
    TxnWIMeta::RWList _rwlist;

    // used to call other K2 nodes(e.g. the TRH for a txn)
    cpo::CPOClient _cpo;

private:
    // process the given action against the given metadata record
    Status _onAction(Action action, TxnWIMeta& twim);

    // helper handlers for individual states
    Status _inProgress(TxnWIMeta& twim);
    Status _inProgressPIP(TxnWIMeta& twim);
    Status _inProgressPIPAborted(TxnWIMeta& twim);
    Status _committed(TxnWIMeta& twim);
    Status _aborted(TxnWIMeta& twim);
    Status _forceFinalize(TxnWIMeta& twim);
    Status _finalizing(TxnWIMeta& twim);
    Status _finalizedPIP(TxnWIMeta& twim);
    Status _finalized(TxnWIMeta& twim, bool success);

    // add a function to the list of background tasks. The function will run when the current chain
    // of background tasks completes.
    template <typename Func>
    void _addBgTask(TxnWIMeta& rec, Func&& func);
}; // class TxnWIMetaManager

}  // namespace k2
