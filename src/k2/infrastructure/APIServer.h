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

#include <skvhttp/mpack/MPackSerialization.h>

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

static const inline std::vector<String> ContentTypeStrings = {
    "application/x-k2payload",
    "application/x-json",
    "application/x-msgpack",
    "application/octet-stream"
};

struct HTTPPayload {
    String data;
    ContentType ctype;
    std::vector<ContentType> accepts;
};

using APIRawObserver_t = std::function<seastar::future<HTTPPayload>(HTTPPayload&&)>;

template<typename Statuses_T>
using GetStatus_T = typename std::remove_cv<typename std::remove_reference_t< decltype(Statuses_T::S100_Continue) >>;

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

template<typename Statuses_T, typename Request_T, typename Response_T, typename Func>
auto _handleK2PAYLOADRequest(HTTPPayload& req, Func&& observer) {
    using Status_T= typename GetStatus_T<Statuses_T>::type;
    K2LOG_D(log::apisvr, "handling incoming payload request of type {}", k2::type_name<Request_T>());
    auto shp = seastar::make_lw_shared<String>(std::move(req.data));
    Payload payload;
    payload.appendBinary(Binary(shp->data(), shp->size(), seastar::make_deleter([shp]()mutable{})));
    Request_T reqObj{};
    if (!payload.read(reqObj)) {
        return seastar::make_ready_future<std::tuple<Status_T, Response_T>>(Statuses_T::S400_Bad_Request("Unable to parse incoming bytes as K2PAYLOAD"), Response_T{});
    };
    return observer(std::move(reqObj));
}

template<typename Statuses_T, typename Request_T, typename Response_T, typename Func>
auto _handleMSGPACKRequest(HTTPPayload& req, Func&& observer) {
    using Status_T= typename GetStatus_T<Statuses_T>::type;
    {
        Request_T newt{};
        skv::http::MPackWriter wtr;
        wtr.write(newt);
        skv::http::Binary b;
        auto fr = wtr.flush(b);
        K2LOG_D(log::apisvr, "example of incoming msgpack request of type {}, encodes as={}, can be serialized={}", k2::type_name<Request_T>(), String(b.data(), b.size()), fr);
    }
    K2LOG_D(log::apisvr, "handling incoming msgpack request of type {}, {}", k2::type_name<Request_T>(), req.data);
    auto shp = seastar::make_lw_shared<String>(std::move(req.data));
    skv::http::Binary bin(shp->data(), shp->size(), [shp] {});
    skv::http::MPackReader reader(bin);
    Request_T reqObj{};
    if (!reader.read(reqObj)) {
        K2LOG_W(log::apisvr, "handling incoming msgpack request of type {}, unable to parse", k2::type_name<Request_T>());
        return seastar::make_ready_future<std::tuple<Status_T, Response_T>>(Statuses_T::S400_Bad_Request("Unable to parse incoming bytes as MSGPACK"), Response_T{});
    };
    K2LOG_D(log::apisvr, "handling incoming msgpack request of type {}; dispatching to observer", k2::type_name<Request_T>());
    return observer(std::move(reqObj));
}

template<typename Statuses_T, typename Request_T, typename Response_T, typename Func>
auto _handleRequest(HTTPPayload& req, Func&& observer) {
    using Status_T= typename GetStatus_T<Statuses_T>::type;
    K2LOG_D(log::apisvr, "handling incoming request of type {}", k2::type_name<Request_T>());
    if (req.ctype == ContentType::MSGPACK) {
        if constexpr(skv::http::isK2SerializableR<Request_T, skv::http::MPackReader>::value ||
                     skv::http::isTrivialClass<Request_T>::value) {
            return _handleMSGPACKRequest<Statuses_T, Request_T, Response_T>(req, std::move(observer));
        }
        K2LOG_W(log::apisvr, "Type {} is not MSGPACK-serializable", k2::type_name<Request_T>());
        return seastar::make_ready_future<std::tuple<Status_T, Response_T>>(Statuses_T::S400_Bad_Request("type is not MSGPACK serializable"), Response_T{});
    }
    else if (req.ctype == ContentType::K2PAYLOAD) {
        if constexpr (k2::isPayloadSerializableType<Request_T>() || k2::isPayloadCopyableType<Request_T>()) {
            return _handleK2PAYLOADRequest<Statuses_T, Request_T, Response_T>(req, std::move(observer));
        }

        K2LOG_W(log::apisvr, "Type {} is not K2PAYLOAD-serializable", k2::type_name<Request_T>());
        return seastar::make_ready_future<std::tuple<Status_T, Response_T>>(Statuses_T::S400_Bad_Request("type is not K2PAYLOAD serializable"), Response_T{});
    }
    K2LOG_W(log::apisvr, "Unsuported content type: {} in request", req.ctype);
    return seastar::make_ready_future<std::tuple<Status_T, Response_T>>(Statuses_T::S400_Bad_Request("Unsupported request content type"), Response_T{});
}

template <typename Status_T, typename Response_T>
auto _handleK2PAYLOADResponse(std::tuple<Status_T, Response_T>&& result) {
    HTTPPayload response;
    response.ctype = ContentType::K2PAYLOAD;

    Payload payload(Payload::DefaultAllocator());
    auto&& [status, resp] = std::move(result);
    payload.write(status);
    payload.write(resp);
    payload.truncateToCurrent();
    for (auto&& buf : payload.release()) {
        response.data.append(buf.get(), buf.size());
    }

    return seastar::make_ready_future<HTTPPayload>(std::move(response));
}

template <typename Status_T, typename Response_T>
auto _handleMSGPACKResponse(std::tuple<Status_T, Response_T>&& result) {
    K2LOG_D(log::apisvr, "handling msgpack response of type {}", k2::type_name<Response_T>());
    HTTPPayload response;
    response.ctype = ContentType::MSGPACK;

    skv::http::MPackWriter writer;
    writer.write(result);
    skv::http::Binary bin;
    writer.flush(bin);
    response.data = String(bin.data(), bin.size());
    return seastar::make_ready_future<HTTPPayload>(std::move(response));
}

template <typename Status_T, typename Response_T>
auto _handleResponse(HTTPPayload& req, std::tuple<Status_T, Response_T>&& result) {
    K2LOG_D(log::apisvr, "handling response of type {}", k2::type_name<Response_T>());
    using RT= std::tuple<Status_T, Response_T>;

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

    if (respCT == ContentType::MSGPACK) {
        if constexpr(skv::http::isK2SerializableW<Response_T, skv::http::MPackWriter>::value ||
                     skv::http::isTrivialClass<Response_T>::value) {
            return _handleMSGPACKResponse<Status_T, Response_T>(std::move(result));
        }
        K2LOG_W(log::apisvr, "Type {} is not MSGPACK-serializable", k2::type_name<RT>());
        return seastar::make_exception_future<HTTPPayload>(std::runtime_error("response expects msgpack content type but response type is not MSGPACK serializable"));
    }
    else if (req.ctype == ContentType::K2PAYLOAD) {
        if constexpr (k2::isPayloadSerializableType<Response_T>() || k2::isPayloadCopyableType<Response_T>()) {
            return _handleK2PAYLOADResponse<Status_T, Response_T>(std::move(result));
        }
        K2LOG_W(log::apisvr, "Type {} is not K2PAYLOAD-serializable", k2::type_name<RT>());
        return seastar::make_exception_future<HTTPPayload>(std::runtime_error("response expects K2PAYLOAD content type but response type is not K2PAYLOAD serializable"));
    }
    K2LOG_W(log::apisvr, "Unsuported content type: {} requested", respCT);
    return seastar::make_exception_future<HTTPPayload>(std::runtime_error("unsupported response content type"));
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
    template <class Statuses_T, class Request_T, class Response_T, typename Func>
    void registerAPIObserver(String pathSuffix, String description, Func&& observer) {
        registerRawAPIObserver(std::move(pathSuffix), std::move(description),
        [this, observer=std::move(observer)] (HTTPPayload&& req) {
            // handle content-type translations in the request
            return seastar::do_with(std::move(req), std::move(observer), [this](auto& req, auto& observer) {
                return _handleRequest<Statuses_T, Request_T, Response_T>(req, std::move(observer))
                    .then([this, &req](auto&& resp) {
                        using Status_T= typename GetStatus_T<Statuses_T>::type;
                        return _handleResponse<Status_T, Response_T>(req, std::move(resp));
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
