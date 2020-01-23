#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

using namespace seastar;

namespace k2 {

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
    K2_PAYLOAD_FIELDS(version);
};

class Key {
public:
    String partition_key;
    String row_key;
    K2_PAYLOAD_FIELDS(partition_key, row_key);
};

std::ostream& operator<<(std::ostream& os, const Key& key) {
    return os << key.partition_key << key.row_key;
}

struct PUT_Request {
    Key key;
    Payload value;
    K2_PAYLOAD_FIELDS(key, value);
};

typedef PUT_Request Record;

struct PUT_Response {
    Key key;
    K2_PAYLOAD_FIELDS(key);
};
struct GET_Request {
    Key key;
    K2_PAYLOAD_FIELDS(key);
};

struct GET_Response {
    Key key;
    Payload value;
    Metadata meta;
    K2_PAYLOAD_FIELDS(key, value, meta);
};

typedef GET_Response ReadResult;

enum MessageVerbs : Verb {
    PUT = 100,
    GET = 101,
    GET_DATA_URL = 102
};

class K2TxnOptions{
public:
    int64_t timeout_usecs;
    Timestamp timestamp;
    int64_t priority;
    // auto-retry policy...
};

class WriteResult{
public:
    WriteResult(Status s) : status(s) {}
    Status status;
};
class EndResult{};

class K2TxnHandle {
public:
    K2TxnHandle(TXEndpoint endpoint) : _endpoint(endpoint) {}

    future<ReadResult> read(Key& key) {
        GET_Request request = {.key = key};
        return RPC().callRPC<GET_Request, GET_Response>(MessageVerbs::GET, std::move(request), _endpoint, 1s).
        then([] (auto response) {
            return make_ready_future<ReadResult>(std::move(std::get<1>(response)));
        });
    }

    future<WriteResult> write(Record && record) { 
        return RPC().callRPC<PUT_Request, PUT_Response>(MessageVerbs::PUT, std::move(record), _endpoint, 1s).
        then([] (auto response) {
            return make_ready_future<WriteResult>(std::get<0>(response));
        });
    }

    future<EndResult> end(bool shouldCommit) { (void) shouldCommit; return make_ready_future<EndResult>(); };
private:
    TXEndpoint _endpoint;
};

class K23SIClientConfig {
public:
    K23SIClientConfig(){};
};

class K23SIClient {
public:
    K23SIClient(K23SIClientConfig &, TXEndpoint endpoint): _remote_endpoint(endpoint){};
    TXEndpoint _remote_endpoint;

    future<K2TxnHandle> beginTxn(const K2TxnOptions&) {
        return make_ready_future<K2TxnHandle>(K2TxnHandle(_remote_endpoint));
    };
};

} // namespace k2

/*
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
    .then([&](K2TxnHandle t) {
        return
        when_all(t.read(keyFrom), t.read(keyTo))
        .then([&](auto results) {
            Account aFrom(std::get<0>(results).get0().record);
            Account aTo(std::get<1>(results).get0().record);

            aFrom.balance -= amount;
            aTo.balance += amount;
            return when_all(t.write(aFrom.toRecord()), t.write(aTo.toRecord()));
        })
        .then([&](auto results) {
            if (std::get<0>(results).get0().status.ok() &&
                std::get<1>(results).get0().status.ok()) {
                K2INFO("Committing transaction since all operations succeeded");
                return t.end(true);
            }
            K2ERROR("Aborting transaction due to write failure");
            return t.end(false);
        });
    })
    .then([&](EndResult) {
        K2INFO("Transaction has completed");
    })
    .handle_exception([](auto&& exc){
        // not able to start a transaction
        try {
            std::rethrow_exception(exc);
        } catch (const std::exception &e) {
            K2ERROR("Transaction failed: " << e.what());
        }
    })
    .wait(); // make sure we complete.
    return 0;
}
*/
