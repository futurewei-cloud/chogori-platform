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

#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include <k2/transport/BaseTypes.h>

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>

namespace k2 {
namespace log {
inline thread_local k2::logging::Logger apisvr("k2::api_server");
}

enum class ContentType: uint8_t {
    K2PAYLOAD=0,
    JSON,
    MSGPACK,
    UNKNOWN
};

static const inline std::vector<String> ContentTypeStrings = {"application/x-k2payload", "application/x-json", "application/x-msgpack", "application/octet-stream"};

struct HTTPPayload {
    String data;
    ContentType ctype;
    std::vector<ContentType> accepts;
};

using APIRawObserver_t = std::function<seastar::future<HTTPPayload>(HTTPPayload&&)>;

// Helper class for registering HTTP routes
class APIRouteHandler : public seastar::httpd::handler_base  {
public:
    template<typename HandlerFunc>
    APIRouteHandler(HandlerFunc&& h) : _handler(std::forward<HandlerFunc>(h)) {}

    seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const String& path,
        std::unique_ptr<seastar::httpd::request> req, std::unique_ptr<seastar::httpd::reply> rep) override;

private:
    APIRawObserver_t _handler;
};

// This class is used to setup an API over HTTP and provide convience methods to expose API methods.
// Its intended use is to expose normal K2 RPCs over an easy to use interface
// Should be used as an applet and the listen IP and port will depend on tcp_endpoints command line
class APIServer {
struct Route {
    String suffix;
    String descr;
};
private:

template<typename Request_T, typename Response_T, typename Func>
auto _handleRequest(HTTPPayload& req, Func&& observer) {
    switch (req.ctype) {
        case ContentType::K2PAYLOAD: {
            auto shp = seastar::make_lw_shared<String>(std::move(req.data));
            Payload payload;
            payload.appendBinary(Binary(shp->data(), shp->size(), seastar::make_deleter([shp=std::move(shp)]()mutable{})));
            Request_T reqObj{};
            if (!payload.read(reqObj)) {
                return seastar::make_exception_future<std::tuple<k2::Status, Response_T>>(std::runtime_error("Unable to parse incoming bytes as K2PAYLOAD"));
            };
            return observer(std::move(reqObj));
        } break;
        default:
            return seastar::make_exception_future<std::tuple<k2::Status, Response_T>>(std::runtime_error("Unsupported content type"));
    }
}

template <typename Response_T>
auto _handleResponse(HTTPPayload& req, std::tuple<k2::Status, Response_T>&& result) {
    // handle content-type translation for the response
    // default is K2PAYLOAD or the first one the caller accepts
    ContentType respCT{req.accepts.size() > 0 ? req.accepts[0] : ContentType::K2PAYLOAD};
    for (auto& ct : req.accepts) {
        // see if we can match the incoming content type
        if (ct == req.ctype) {
            respCT = ct;
            break;
        }
    }
    HTTPPayload response;
    response.ctype = respCT;

    switch (respCT) {
        case ContentType::K2PAYLOAD: {
            Payload payload(Payload::DefaultAllocator());
            auto&& [status, resp] = std::move(result);
            payload.write(status);
            payload.write(resp);
            payload.truncateToCurrent();
            for (auto&& buf : payload.release()) {
                response.data.append(buf.get(), buf.size());
            }
        } break;
        default:
            return seastar::make_exception_future<HTTPPayload>(std::runtime_error("Unsupported accepts-content-type"));
    }
    return seastar::make_ready_future<HTTPPayload>(std::move(response));
}

public:
    APIServer() : _server("HTTP API Server") {}
    APIServer(String name) : _server(name) {}

    // initialize server.
    seastar::future<> start();

    // this method should be called to stop the server (usually on engine exit)
    seastar::future<> gracefulStop();

    // All HTTP paths will be registered under api/
    // A GET on api/ will show each path registered this way with a description
    // Request and Response types must be K2_PAYLOAD_SERIALIZABLE
    template <class Request_T, class Response_T>
    void registerAPIObserver(String pathSuffix, String description,
                             RPCRequestObserver_t<Request_T, Response_T> observer) {
        registerRawAPIObserver(std::move(pathSuffix), std::move(description),
        [this, observer=std::move(observer)] (HTTPPayload&& req) {
            // handle content-type translations in the request
            return seastar::do_with(std::move(req), std::move(observer), [this] (auto& req, auto& observer) {
                return _handleRequest<Request_T, Response_T>(req, std::move(observer))
                    .then([this, &req] (auto&& resp) {
                        return _handleResponse<Response_T>(req, std::move(resp));
                    });
            });
        });
    }

    void registerRawAPIObserver(String pathSuffix, String description, APIRawObserver_t observer);
    void deregisterAPIObserver(String pathSuffix);

private:
    // API server will listen on the IP for our core, with port=tcp_port + the offset
    static constexpr uint16_t API_PORT_OFFSET = 10000;

    seastar::httpd::http_server _server;
    std::vector<Route> _registeredRoutes;

    seastar::future<> addRoutes();
    String getCurrentRoutes();
}; // class APIServer
} // k2 namespace
