#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

using namespace seastar;

namespace k2 {

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

struct ReadResult {
    Key key;
    Payload value;
    Metadata meta;
    Status status;
};

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
    K2TxnHandle(TXEndpoint endpoint) noexcept : _endpoint(endpoint) {}
    K2TxnHandle(K2TxnHandle&& from) noexcept : _endpoint(std::move(from._endpoint)) {}
    ~K2TxnHandle() noexcept {}

    future<ReadResult> read(Key&& key) {
        return RPC().callRPC<Key, GET_Response>(MessageVerbs::GET, std::move(key), _endpoint, 1s).
        then([] (auto response) {
            struct ReadResult userResponse = {};
            userResponse.key = std::move(std::get<1>(response).key);
            userResponse.value = std::move(std::get<1>(response).value);
            userResponse.meta = std::move(std::get<1>(response).meta);
            userResponse.status = std::move(std::get<0>(response));
            return make_ready_future<ReadResult>(std::move(userResponse));
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
    K23SIClient(const K23SIClientConfig &) {};
    K23SIClient(const K23SIClientConfig &, TXEndpoint endpoint): _remote_endpoint(endpoint){};
    TXEndpoint _remote_endpoint;

    future<K2TxnHandle> beginTxn(const K2TxnOptions& options) {
        (void) options;
        return make_ready_future<K2TxnHandle>(K2TxnHandle(_remote_endpoint));
    };
};

} // namespace k2
