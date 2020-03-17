#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <k2/common/Log.h>
#include <k2/common/Common.h>

using namespace seastar;
using namespace k2;

typedef String Key;
typedef String Value;

class Timestamp {
public:
    int64_t getSequence(){ return 0;};
    int64_t getStart() { return 0; };
    int64_t getEnd() { return 0; };
    int64_t getTSOID() { return 0; };
};

class Metadata {
public:
    Timestamp last_updated;
    int64_t version;
};


class Record {
public:
    Key key;
    Value value;
    Metadata meta;
};

class K2TxnOptions{
public:
    int64_t timeout_usecs;
    Timestamp timestamp;
    int64_t priority;
    // auto-retry policy...
};

class K2Status {
public:
    bool ok() { return false;}
};

class ReadResult{
public:
    Record record;
};
class WriteResult{
public:
    K2Status status;
};
class EndResult{};

class K2TxnHandle {
public:
    future<ReadResult> read(Key &) { return make_ready_future<ReadResult>(); };
    future<WriteResult> write(Record &&) { return make_ready_future<WriteResult>(); };
    future<EndResult> end(bool shouldCommit) { (void) shouldCommit; return make_ready_future<EndResult>(); };
};

class K2ClientConfig {
public:
    K2ClientConfig(){};
};

class K2Client {
public:
    K2Client(K2ClientConfig &){};
};

class K2TxnLib {
public:
    K2TxnLib(K2Client &){};
    future<K2TxnHandle> begin(const K2TxnOptions&){
        return make_ready_future<K2TxnHandle>();
    };
};


// User class for Account object
class Account {
public:
    double balance;
    Account(Record &&) {};
    Record toRecord() { Record result; return result;};
};

int main(int argc, char **argv) {
    (void) argv;
    (void) argc;
    K2ClientConfig clientConfig;
    K2Client k2client(clientConfig);
    K2TxnLib txnlib(k2client);
    K2TxnOptions opts;

    Key keyFrom("Account-from"), keyTo("Account-to");
    double amount = 100;

    txnlib.begin(opts)
    .then([&](K2TxnHandle&& t) {
        return
        when_all(t.read(keyFrom), t.read(keyTo))
        .then([&](auto&& results) {
            Account aFrom(std::get<0>(results).get0().record);
            Account aTo(std::get<1>(results).get0().record);

            aFrom.balance -= amount;
            aTo.balance += amount;
            return when_all(t.write(aFrom.toRecord()), t.write(aTo.toRecord()));
        })
        .then([&](auto&& results) {
            if (std::get<0>(results).get0().status.ok() &&
                std::get<1>(results).get0().status.ok()) {
                K2INFO("Committing transaction since all operations succeeded");
                return t.end(true);
            }
            K2ERROR("Aborting transaction due to write failure");
            return t.end(false);
        });
    })
    .then([&](EndResult&&) {
        K2INFO("Transaction has completed");
    })
    .handle_exception([](auto exc){
        // not able to start a transaction
        K2ERROR_EXC("Transaction failed", exc);
    })
    .wait(); // make sure we complete.
    return 0;
}
