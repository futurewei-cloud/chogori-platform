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

#include <k2/dto/K23SI.h>
#include <k2/cpo/client/CPOClient.h>
#include <boost/intrusive/list.hpp>
#include <seastar/core/shared_ptr.hh>
#include "Config.h"
#include "Persistence.h"

namespace k2 {
class K23SIPartitionModule;

namespace nsbi = boost::intrusive;

// A Transaction record
struct TxnRecord {
    dto::TxnId txnId;

    // the keys to which this transaction wrote. These are delivered as part of the End request and we have to ensure
    // that the corresponding write intents are converted appropriately
    std::vector<dto::Key> writeKeys;

    // Expiry time point for retention window - these are driven off each TSO clock update
    dto::Timestamp rwExpiry;
    nsbi::list_member_hook<> rwLink;

    // for the heartbeat timer set - these are driven off local time
    TimePoint hbExpiry;
    nsbi::list_member_hook<> hbLink;

    // this link and future are used to track this transaction when it enters background processing(e.g. finalize or delete)
    nsbi::list_member_hook<> bgTaskLink;
    seastar::future<> bgTaskFut = seastar::make_ready_future();

    bool syncFinalize = false;

    friend std::ostream& operator<<(std::ostream& os, const TxnRecord& rec) {
        os << "{txnId=" << rec.txnId << ", writeKeys=[";
        os << rec.writeKeys.size();
        //for (auto& key: rec.writeKeys) {
        //    os << key << ",";
        //}
        os << "], rwExpiry=" << rec.rwExpiry << ", hbExpiry=" << rec.hbExpiry << ", syncfin=" << rec.syncFinalize << "}";
        return os;
    }

    dto::TxnRecordState state = dto::TxnRecordState::Created;

    K2_PAYLOAD_FIELDS(txnId, writeKeys, state);

    // The last action on this TR (the action that put us into the above state)
    enum class Action : uint8_t {
        Nil = 0,
        onCreate,
        onForceAbort,
        onRetentionWindowExpire,
        onHeartbeat,
        onHeartbeatExpire,
        onEndCommit,
        onEndAbort,
        onFinalizeComplete
    };

    friend std::ostream& operator<<(std::ostream& os, const Action& action) {
        const char* straction = "bad action";
        switch (action) {
            case Action::Nil: straction= "nil"; break;
            case Action::onCreate: straction= "on_create"; break;
            case Action::onForceAbort: straction= "on_force_abort"; break;
            case Action::onRetentionWindowExpire: straction= "on_retention_window_expire"; break;
            case Action::onHeartbeat: straction= "on_heartbeat"; break;
            case Action::onHeartbeatExpire: straction= "on_heartbeat_expire"; break;
            case Action::onEndCommit: straction= "on_end_commit"; break;
            case Action::onEndAbort: straction= "on_end_abort"; break;
            case Action::onFinalizeComplete: straction= "on_finalize_complete"; break;
            default: break;
        }
        return os << straction;
    }

    typedef nsbi::list<TxnRecord, nsbi::member_hook<TxnRecord, nsbi::list_member_hook<>, &TxnRecord::rwLink>> RWList;
    typedef nsbi::list<TxnRecord, nsbi::member_hook<TxnRecord, nsbi::list_member_hook<>, &TxnRecord::hbLink>> HBList;
    typedef nsbi::list<TxnRecord, nsbi::member_hook<TxnRecord, nsbi::list_member_hook<>, &TxnRecord::bgTaskLink>> BGList;

    void unlinkHB(HBList& hblist);
    void unlinkRW(RWList& hblist);
    void unlinkBG(BGList& hblist);
};  // class TR

// take care of
// - tr state transitions
// - persisting tr state
// - recovery of tr state
// - heartbeat tr transition
// - txn finalization
// - txn recovery
class TxnManager {
public: // lifecycle
    TxnManager();
    ~TxnManager();

    // When started, we need to be told:
    // - the current retentionTimestamp
    // - the heartbeat interval for the collection
    seastar::future<> start(const String& collectionName, dto::Timestamp rts, Duration hbDeadline);

    // called when
    seastar::future<> gracefulStop();

    // called to update the retention timestamp when the server refreshes from TSO
    // We cache this value and use it to expire transactions when they are outside retention window.
    void updateRetentionTimestamp(dto::Timestamp rts);

    TxnRecord* getTxnRecordNoCreate(const dto::TxnId& txnId);
    // returns the record for an id. Creates a new record in Created state if one does not exist
    TxnRecord& getTxnRecord(const dto::TxnId& txnId);
    TxnRecord& getTxnRecord(dto::TxnId&& txnId);

    // delivers the given action for the given transaction.
    // If there is a failure we return an exception future with:
    // ClientError: indicates the client has attempted an invalid action and so the transaction should abort
    // ServerError: indicates that we had trouble processing the transaction. The client should abort.
    seastar::future<> onAction(TxnRecord::Action action, dto::TxnId txnId);

    // onAction can complete successfully or with one of these errors
    struct ClientError: public std::exception{
        virtual const char* what() const noexcept override { return "client error"; }
    };
    struct ServerError: public std::exception{
        virtual const char* what() const noexcept override { return "server error"; }
    };

private: // methods driving the state machine
    // helper state handlers for each state we want to enter.
    // The contract here is that the state transition has been validated (i.e. the state transition is an allowed one)
    // but no other validation has been performed. Upon problem, we return either a ClientError or ServerError
    seastar::future<> _inProgress(TxnRecord& rec);
    seastar::future<> _forceAborted(TxnRecord& rec);
    seastar::future<> _end(TxnRecord& rec, dto::TxnRecordState state);
    seastar::future<> _deleted(TxnRecord& rec);
    seastar::future<> _heartbeat(TxnRecord& rec);
    seastar::future<> _finalizeTransaction(TxnRecord& rec, FastDeadline deadline);

    TxnRecord& _createRecord(dto::TxnId txnId);

private: // fields
    friend class K23SIPartitionModule;

    // Expiry lists. The order in the list is ascending so that the oldest item would be in the front
    TxnRecord::RWList _rwlist;
    TxnRecord::HBList _hblist;

    // this list holds the transactions which are doing some background task.
    TxnRecord::BGList _bgTasks;

    // heartbeats checks are driven off single timer.
    seastar::timer<> _hbTimer;
    seastar::future<> _hbTask = seastar::make_ready_future();

    // the primary store for transaction records
    std::unordered_map<dto::TxnId, TxnRecord> _transactions;

    // the configuration for the k23si module
    K23SIConfig _config;

    // the collection-wide deadline for heartbeating of transactions
    Duration _hbDeadline;

    // this is the retention window timestamp we should use for new transactions
    dto::Timestamp _retentionTs;

    Persistence _persistence;

    bool _stopping = false;

    String _collectionName;
    CPOClient _cpo;
}; // class TxnManager

}  // namespace k2
