//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>
#include <seastar/core/sleep.hh>

namespace k2 {
// implements an example RPC Service which can send/receive messages

struct PUT_Request {
    String key;
    String value;
    K2_PAYLOAD_FIELDS(key, value);
};
struct PUT_Response {
    String key;
    K2_PAYLOAD_FIELDS(key);
};
struct GET_Request {
    String key;
    K2_PAYLOAD_FIELDS(key);
};
struct GET_Response {
    String key;
    String value;
    K2_PAYLOAD_FIELDS(key, value);
};

enum MessageVerbs : Verb {
    PUT = 100,
    GET = 101
};

class KVService {
public:  // application lifespan
    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2INFO("stop");
        return seastar::make_ready_future<>();
    }

    // called after construction
    void start() {
        K2INFO("Registering message handlers");

        RPC().registerRPCObserver<PUT_Request, PUT_Response>(MessageVerbs::PUT, [this](PUT_Request&& request) {
            K2INFO("Received put for key: " << request.key);
            _cache[request.key] = request.value;
            PUT_Response response{.key=std::move(request.key)};
            return RPCResponse(Statuses::S200_OK("put accepted"), std::move(response));
        });

        RPC().registerRPCObserver<GET_Request, GET_Response>(MessageVerbs::GET, [this](GET_Request&& request) {
            K2INFO("Received get for key: " << request.key);
            GET_Response response;
            response.key=request.key;
            auto iter = _cache.find(request.key);
            if (iter != _cache.end()) {
                response.value = iter->second;
            }
            return RPCResponse(Statuses::S200_OK("get accepted"), std::move(response));
        });
    }
private:
    std::map<String, String> _cache;
};  // class Service

class KVClientTest {
public:
    seastar::future<> stop() {
        K2INFO("stop");
        return seastar::make_ready_future<>();
    }

    void start() {
        (void)seastar::sleep(1s)
        .then([] {
            K2INFO("putting record");
            PUT_Request request{.key="Key1", .value="Value1"};
            auto ep = RPC().getServerEndpoint(TCPRPCProtocol::proto);
            K2INFO("found endpoint: " << ep->getURL());
            return RPC().callRPC<PUT_Request, PUT_Response>(MessageVerbs::PUT, request, *ep, 1s);
        })
        .then([](auto&& resp) {
            K2INFO("Received PUT response with status: " << std::get<0>(resp));

            K2INFO("getting record");
            GET_Request request{.key = "Key1"};
            return RPC().callRPC<GET_Request, GET_Response>(MessageVerbs::GET, request, *RPC().getServerEndpoint(TCPRPCProtocol::proto), 1s);
        })
        .then([](auto&& resp) {
            K2INFO("Received GET response with status: " << std::get<0>(resp) << ", value=" << std::get<1>(resp).value);
        });
    }
};
}  // namespace k2

int main(int argc, char** argv) {
    k2::App app;
    app.addApplet<k2::KVService>();
    app.addApplet<k2::KVClientTest>();
    return app.start(argc, argv);
}
