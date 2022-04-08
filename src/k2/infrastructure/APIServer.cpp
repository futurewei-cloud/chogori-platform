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

#include "APIServer.h"
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/dto/Collection.h>

#include <seastar/core/shared_ptr.hh>
#include <seastar/net/socket_defs.hh>

namespace k2 {

class get_routes_handler : public seastar::httpd::handler_base  {
public:
    get_routes_handler(std::function<String()> h) : _handler(std::move(h)) {}

    seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const String& path,
        std::unique_ptr<seastar::httpd::request> req, std::unique_ptr<seastar::httpd::reply> rep) override {
        (void) path;
        (void) req;
        rep->write_body("txt", _handler());
        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
    }

private:
    std::function<String()> _handler;
};

seastar::future<std::unique_ptr<seastar::httpd::reply>> api_route_handler::handle(const String& path,
    std::unique_ptr<seastar::httpd::request> req, std::unique_ptr<seastar::httpd::reply> rep)  {
    (void) path;

    // seastar returns a 200 status code by default, which is what we want. The k2 status code will
    // be embedded in the returned json object
    return _handler(req->content)
    .then([rep=std::move(rep)] (nlohmann::json&& json) mutable {
        rep->write_body("json",  [json=std::move(json)] (auto&& os)  mutable {
            return do_with(std::move(os),  json=std::move(json), [] (auto& os, auto& json) {
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

seastar::future<>
APIServer::start() {
    K2LOG_I(log::apisvr, "starting JSON API server on port");

    int coreID = seastar::this_shard_id();
    seastar::ipv4_addr listenAddr;
    bool parsed = false;
    if (size_t(coreID) < _tcp_endpoints().size()) {
        auto ep = k2::TXEndpoint::fromURL(_tcp_endpoints()[coreID], BinaryAllocator());
        if (ep) {
            parsed = true;
            listenAddr = seastar::ipv4_addr(ep->ip, uint16_t(ep->port + API_PORT_OFFSET));
        }

        // Try parsing as port only
        if (!parsed) {
            try {
                parsed = true;
                auto port = std::stoi(_tcp_endpoints()[coreID]);
                listenAddr = seastar::socket_address((uint16_t)(port + API_PORT_OFFSET));
            } catch (...) {
            }
        }
    }

    if (!parsed) {
        K2LOG_I(log::apisvr, "No IP/Port provided for API server, aborting server start()");
        return seastar::make_ready_future<>();
    }

    return add_routes()
    .then([this, listenAddr=std::move(listenAddr)]() {
        return _server.listen(listenAddr);
    });
}

seastar::future<> APIServer::gracefulStop() {
    K2LOG_I(log::apisvr, "Stopping APIServer");
    return  _server.stop();
}

seastar::future<> APIServer::add_routes() {
    _server._routes.put(seastar::httpd::GET, "/api",
            new get_routes_handler([this] () { return get_current_routes();}));
    return seastar::make_ready_future<>();
}

String APIServer::get_current_routes() {
    String routes;
    for (const std::pair<String, String>& route : _registered_routes) {
        routes += route.first + ": " + route.second + "\n";
    }

    return routes;
}

void APIServer::registerRawAPIObserver(String pathSuffix, String description, APIRawObserver_t observer) {
    _registered_routes.emplace_back(std::make_pair(pathSuffix, description));

    _server._routes.put(seastar::httpd::POST, "/api/" + pathSuffix, new api_route_handler(
    [observer=std::move(observer)] (const String& jsonRequest) {
        nlohmann::json request = nlohmann::json::parse(jsonRequest.cbegin(), jsonRequest.cend());
        return observer(std::move(request));
    }));
}

void APIServer::deregisterAPIObserver(String pathSuffix) {
    bool found = false;
    auto it = _registered_routes.begin();
    for (; it != _registered_routes.end(); ++it) {
        auto& [path, descrip] = *it;
        if (path == pathSuffix) {
            _registered_routes.erase(it);
            found = true;
            break;
        }
    }

    if (!found) {
        return;
    }

    auto handler =  _server._routes.drop(seastar::httpd::POST, "/api/" + pathSuffix);
    delete handler;
}

}// namespace k2
