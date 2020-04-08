#pragma once

#include <k2/dto/K23SI.h>
#include <k2/cpo/client/CPOClient.h>
#include <boost/intrusive/list.hpp>
#include <seastar/core/shared_ptr.hh>
#include "Config.h"
#include "Persistence.h"

namespace k2 {
namespace nsbi = boost::intrusive;

// Complete unique identifier of a transaction in the system
struct TxnId {
    // this is the routable key for the TR - we can route requests for the TR (i.e. PUSH)
    // based on the partition map of the collection.
    dto::Key trh;
    // the MTR for the transaction
    dto::K23SI_MTR mtr;
    size_t hash() const;
    bool operator==(const TxnId& o) const;
    bool operator!=(const TxnId& o) const;
    friend std::ostream& operator<<(std::ostream& os, const TxnId& txnId) {
        return os << "{trh=" << txnId.trh << ", mtr=" << txnId.mtr <<"}";
    }
};
} // ns k2

// Define std::hash for TxnIds so that we can use them in hash maps/sets
namespace std {
template <>
struct hash<k2::TxnId> {
    size_t operator()(const k2::TxnId& txnId) const {
        return txnId.hash();
    }
};  // hash
}  // namespace std

namespace k2 {

// A record in the cache.
struct DataRecord {
    dto::Key key;
    SerializeAsPayload<Payload> value;
    bool isTombstone = false;
    TxnId txnId;
    enum Status: uint8_t {
        WriteIntent,  // the record hasn't been committed/aborted yet
        Committed     // the record has been committed and we should use the key/value
        // aborted WIs don't need state - as soon as we learn that a WI has been aborted, we remove it
    } status;
};

// A Transaction record
struct TxnRecord {
    TxnId txnId;

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

    friend std::ostream& operator<<(std::ostream& os, const TxnRecord& rec) {
        os << "{txnId=" << rec.txnId << ", writeKeys=[";
        for (auto& key: rec.writeKeys) {
            os << key << ",";
        }
        os << "], rwExpiry=" << rec.rwExpiry << ", hbExpiry=" << rec.hbExpiry << "}";
        return os;
    }

    // TR state
    enum class State : uint8_t {
        Created = 0,
        InProgress,
        ForceAborted,
        Aborted,
        Committed,
        Deleted
    } state = State::Created;

    friend std::ostream& operator<<(std::ostream& os, const State& st) {
        const char* strstate = "bad state";
        switch (st) {
            case State::Created: strstate= "created"; break;
            case State::InProgress: strstate= "in_progress"; break;
            case State::ForceAborted: strstate= "force_aborted"; break;
            case State::Aborted: strstate= "aborted"; break;
            case State::Committed: strstate= "committed"; break;
            case State::Deleted: strstate= "deleted"; break;
            default: break;
        }
        return os << strstate;
    }

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
    seastar::future<> stop();

    // called to update the retention timestamp when the server refreshes from TSO
    // We cache this value and use it to expire transactions when they are outside retention window.
    void updateRetentionTimestamp(dto::Timestamp rts);

    // returns the record for an id. Creates a new record in Created state if one does not exist
    TxnRecord& getTxnRecord(const TxnId& txnId);
    TxnRecord& getTxnRecord(TxnId&& txnId);

    // delivers the given action for the given transaction.
    // If there is a failure we return an exception future with:
    // ClientError: indicates the client has attempted an invalid action and so the transaction should abort
    // ServerError: indicates that we had trouble processing the transaction. The client should abort.
    seastar::future<> onAction(TxnRecord::Action action, TxnId txnId);

    // onAction can complete successfully or with one of these errors
    class ClientError: public std::exception{};
    class ServerError: public std::exception{};

private: // methods driving the state machine
    // helper state handlers for each state we want to enter.
    // The contract here is that the state transition has been validated (i.e. the state transition is an allowed one)
    // but no other validation has been performed. Upon problem, we return either a ClientError or ServerError
    seastar::future<> _inProgress(TxnRecord& rec);
    seastar::future<> _forceAborted(TxnRecord& rec);
    seastar::future<> _aborted(TxnRecord& rec);
    seastar::future<> _committed(TxnRecord& rec);
    seastar::future<> _deleted(TxnRecord& rec);
    seastar::future<> _heartbeat(TxnRecord& rec);
    seastar::future<> _finalizeTransaction(TxnRecord& rec, FastDeadline deadline);

    TxnRecord& _createRecord(TxnId txnId);

private: // fields
    // Expiry lists. The order in the list is ascending so that the oldest item would be in the front
    TxnRecord::RWList _rwlist;
    TxnRecord::HBList _hblist;

    // this list holds the transactions which are doing some background task.
    TxnRecord::BGList _bgTasks;

    // heartbeats checks are driven off single timer.
    seastar::timer<> _hbTimer;
    seastar::future<> _hbTask = seastar::make_ready_future();

    // the primary store for transaction records
    std::unordered_map<TxnId, TxnRecord> _transactions;

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
