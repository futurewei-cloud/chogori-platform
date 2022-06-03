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
#include <k2/logging/Log.h>
#include <k2/common/Common.h>
#include <k2/dto/Collection.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/TCPRPCProtocol.h>

#include <seastar/core/shared_ptr.hh>
#include <seastar/net/socket_defs.hh>

namespace k2 {

class GetRoutesHandler : public seastar::httpd::handler_base  {
public:
    GetRoutesHandler(std::function<String()> h) : _handler(std::move(h)) {}

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

seastar::future<std::unique_ptr<seastar::httpd::reply>> APIRouteHandler::handle(const String&,
    std::unique_ptr<seastar::httpd::request> req, std::unique_ptr<seastar::httpd::reply> rep)  {

    // seastar returns a 200 status code by default, which is what we want. The k2 status code will
    // be embedded in the returned json object
    ContentType ctype = ContentType::UNKNOWN;
    auto& ctheader = req->_headers["Content-Type"];
    for (size_t i = 0; i < ContentTypeStrings.size(); ++i) {
        if (ContentTypeStrings[i] == ctheader) {
            ctype = ContentType(i);
            break;
        }
    }

    auto& acceptHeader = req->_headers["Accept"];
    std::vector<ContentType> accepts;
    {
        auto strToCtype = [] (const String& hdr) {
            for (size_t i = 0; i < ContentTypeStrings.size(); ++i) {
                if (hdr == ContentTypeStrings[i]) {
                    return ContentType(i);
                }
            }
            return ContentType::UNKNOWN;
        };

        const String delim(", ");
        auto start = 0U;
        auto end = acceptHeader.find(delim);
        if (end == String::npos) {
            accepts.push_back(strToCtype(acceptHeader));
        }
        else {
            // multi-accepts header (comma-separated)
            while (end != std::string::npos) {
                accepts.push_back(strToCtype(acceptHeader.substr(start, end - start)));
                start = end + delim.length();
                end = acceptHeader.find(delim, start);
            }
        }
    }

    HTTPPayload hp{
        .data=std::move(req->content),
        .ctype=ctype,
        .accepts=std::move(accepts)
    };

    return _handler(std::move(hp))
    .then([rep=std::move(rep)] (auto&& payload) mutable {
        const String& ctype= ContentTypeStrings[to_integral(payload.ctype)];
        rep->write_body(ctype, [payload=std::move(payload)] (auto&& os)  mutable {
            return do_with(std::move(os), payload=std::move(payload), [] (auto& os, auto& payload) {
                return os.write(payload.data)
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
    auto tcpEp = RPC().getServerEndpoint(k2::TCPRPCProtocol::proto);
    if (!tcpEp) {
        K2LOG_E(log::apisvr, "Unable to start APIServer since this core does not support TCP protocol");
        return seastar::make_exception_future(std::runtime_error("unable to initialize APIServer"));
    }
    auto myPort = uint16_t(tcpEp->port + API_PORT_OFFSET);
    K2LOG_I(log::apisvr, "starting HTTP API server on port {}", myPort);
    auto listenAddr = seastar::ipv4_addr(tcpEp->ip, myPort);

    return addRoutes()
        .then([this, listenAddr=std::move(listenAddr)]() {
            return _server.listen(listenAddr);
        });
}

seastar::future<> APIServer::gracefulStop() {
    K2LOG_I(log::apisvr, "Stopping APIServer");
    return  _server.stop();
}

seastar::future<> APIServer::addRoutes() {
    _server._routes.put(seastar::httpd::GET, "/api",
            new GetRoutesHandler([this] () { return getCurrentRoutes();}));
    return seastar::make_ready_future<>();
}

String APIServer::getCurrentRoutes() {
    String routes;
    for (auto& route : _registeredRoutes) {
        routes += route.suffix + ": " + route.descr + "\n";
    }

    return routes;
}

void APIServer::registerRawAPIObserver(String pathSuffix, String description, APIRawObserver_t observer) {
    _registeredRoutes.emplace_back(Route{.suffix=pathSuffix, .descr=description});

    _server._routes.put(seastar::httpd::POST, "/api/" + pathSuffix, new APIRouteHandler(
        [observer=std::move(observer)] (auto&& httpPayload) {
            return observer(std::move(httpPayload));
        }));
}

void APIServer::deregisterAPIObserver(String pathSuffix) {
    bool found = false;
    auto it = _registeredRoutes.begin();
    for (; it != _registeredRoutes.end(); ++it) {
        if (it->suffix == pathSuffix) {
            _registeredRoutes.erase(it);
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
