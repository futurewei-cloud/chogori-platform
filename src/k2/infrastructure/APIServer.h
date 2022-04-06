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

using APIRawObserver_t = std::function<seastar::future<nlohmann::json>(nlohmann::json&& request)>;

// Helper class for registering HTTP routes
class api_route_handler : public seastar::httpd::handler_base  {
public:
    api_route_handler(std::function<seastar::future<nlohmann::json>(const String&)> h) : _handler(std::move(h)) {}

    seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const String& path,
        std::unique_ptr<seastar::httpd::request> req, std::unique_ptr<seastar::httpd::reply> rep) override {
        (void) path;

        // seastar returns a 200 status code by default, which is what we want. The k2 status code will
        // be embedded in the returned json object
        return _handler(req->content)
        .then([rep=std::move(rep)] (nlohmann::json&& json) mutable {
            rep->write_body("json",  [json=std::move(json)] (auto&& os)  {
                return do_with(std::move(os),  [json=std::move(json)] (auto& os) {
                    // Using json dump to write instead of << operator because seastar dosn't
                    // implement << operator. Also nlohmann::json just uses dump() method internally.
                    // in << operator implementation.
                    // https://json.nlohmann.me/api/basic_json/operator_ltlt/#operatorbasic_json
                    return os.write(json.dump())
                    .finally([& os] {
                        return os.close();
                    });
                });
            });
            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        });
    }

private:
    std::function<seastar::future<nlohmann::json>(const String&)> _handler;
};

// This class is used to setup a Json API over HTTP and provide convience methods to expose API methods.
// Its intended use is to expose normal K2 RPCs over an easy to use interface
// Should be used as an applet and the listen IP and port will depend on tcp_endpoints command line
class APIServer {
public:
    APIServer() : _server("Json API Server") {}
    APIServer(String name) : _server(name) {}

    // initialize server.
    seastar::future<> start();

    // this method should be called to stop the server (usually on engine exit)
    seastar::future<> gracefulStop();

    // All HTTP paths will be registered under api/
    // A GET on api/ will show each path registered this way with a description
    // Request and Response types must be convertable to JSON
    template <class Request_t, class Response_t>
    void registerAPIObserver(String pathSuffix, String description,
                             RPCRequestObserver_t<Request_t, Response_t> observer) {
        _registered_routes.emplace_back(std::make_pair(pathSuffix, description));

        _server._routes.put(seastar::httpd::POST, "/api/" + pathSuffix, new api_route_handler(
        [observer=std::move(observer)] (const String& jsonRequest) {
           Request_t request = nlohmann::json::parse(jsonRequest);
           return observer(std::move(request))
           .then([] (std::tuple<k2::Status, Response_t>&& fullResponse) {
              auto [status, response] = fullResponse;
              nlohmann::json jsonResponse;
              jsonResponse["status"] = status;
              jsonResponse["response"] = response;
              return seastar::make_ready_future<nlohmann::json>(std::move(jsonResponse));
           });
        }));
    }

    void registerRawAPIObserver(String pathSuffix, String description, APIRawObserver_t observer) {
        _registered_routes.emplace_back(std::make_pair(pathSuffix, description));

        _server._routes.put(seastar::httpd::POST, "/api/" + pathSuffix, new api_route_handler(
        [observer=std::move(observer)] (const String& jsonRequest) {
           nlohmann::json request = nlohmann::json::parse(jsonRequest.cbegin(), jsonRequest.cend());
           return observer(std::move(request));
        }));
    }

    void deregisterAPIObserver(String pathSuffix);

private:
    // API server will listen on the IP in _tcp_endpoints (by core ID) on the same port + the offset
    static constexpr uint16_t API_PORT_OFFSET = 10000;
    ConfigVar<std::vector<String>> _tcp_endpoints{"tcp_endpoints"};

    seastar::httpd::http_server _server;
    std::vector<std::pair<String, String>> _registered_routes;

    seastar::future<> add_routes();
    String get_current_routes();
}; // class APIServer
} // k2 namespace
