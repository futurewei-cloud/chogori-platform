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

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>
#include <seastar/core/sleep.hh>

namespace k2 {
// implements an example RPC Service which can send/receive messages
namespace log {
inline thread_local k2::logging::Logger rpcsvc("k2::rpc_service");
}

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
    seastar::future<> gracefulStop() {
        K2LOG_I(log::rpcsvc, "stop");
        return seastar::make_ready_future<>();
    }

    // called after construction
    void start() {
        K2LOG_I(log::rpcsvc, "Registering message handlers");

        RPC().registerRPCObserver<PUT_Request, PUT_Response>(MessageVerbs::PUT, [this](PUT_Request&& request) {
            K2LOG_I(log::rpcsvc, "Received put for key: {}", request.key);
            _cache[request.key] = request.value;
            PUT_Response response{.key=std::move(request.key)};
            return RPCResponse(Statuses::S200_OK("put accepted"), std::move(response));
        });

        RPC().registerRPCObserver<GET_Request, GET_Response>(MessageVerbs::GET, [this](GET_Request&& request) {
            K2LOG_I(log::rpcsvc, "Received get for key: {}", request.key);
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
    seastar::future<> gracefulStop() {
        K2LOG_I(log::rpcsvc, "stop");
        return seastar::make_ready_future<>();
    }

    void start() {
        (void)seastar::sleep(1s)
        .then([] {
            K2LOG_I(log::rpcsvc, "putting record");
            PUT_Request request{.key="Key1", .value="Value1"};
            auto ep = RPC().getServerEndpoint(TCPRPCProtocol::proto);
            K2LOG_I(log::rpcsvc, "found endpoint: {}", ep->getURL());
            return RPC().callRPC<PUT_Request, PUT_Response>(MessageVerbs::PUT, request, *ep, 1s);
        })
        .then([](auto&& resp) {
            K2LOG_I(log::rpcsvc, "Received PUT response with status: {}", std::get<0>(resp));

            K2LOG_I(log::rpcsvc, "getting record");
            GET_Request request{.key = "Key1"};
            return RPC().callRPC<GET_Request, GET_Response>(MessageVerbs::GET, request, *RPC().getServerEndpoint(TCPRPCProtocol::proto), 1s);
        })
        .then([](auto&& resp) {
            K2LOG_I(log::rpcsvc, "Received GET response with status: {}, value={}", std::get<0>(resp), std::get<1>(resp).value);
        });
    }
};
}  // namespace k2

int main(int argc, char** argv) {
    k2::App app("RPCServiceDemo");
    app.addApplet<k2::KVService>();
    app.addApplet<k2::KVClientTest>();
    return app.start(argc, argv);
}
