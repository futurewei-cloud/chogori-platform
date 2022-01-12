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
#include <k2/common/Timer.h>
#include <typeinfo>
namespace k2 {
// implements an example RPC Service which can send/receive messages
namespace log {
inline thread_local k2::logging::Logger echo("k2::rpc_echo");
}

struct Echo_Message {
    String message;
    K2_PAYLOAD_FIELDS(message);
    K2_DEF_FMT(Echo_Message, message);
};

enum MessageVerbs : Verb {
    MES = 102
};

class PingService {
public:  // application lifespan
    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::echo, "stop");
        return seastar::make_ready_future<>();
    }

    // called after construction
    void start() {
        K2LOG_I(log::echo, "Registering message handlers");

        RPC().registerRPCObserver<Echo_Message, Echo_Message>(MessageVerbs::MES, [this](Echo_Message&& request) {
            K2LOG_D(log::echo, "Received ping for message: {}", request.message);
            Echo_Message response;
            response.message=request.message;
            return RPCResponse(Statuses::S200_OK("pong"), std::move(response));
        });
    }
};  // class Service

class PingClient {
private:
    PeriodicTimer _heartbeatTimer;
    ConfigDuration _interval{"interval", 1s};
    ConfigVar<std::vector<String>> _strEndpoints{"endpoints"};
    std::vector<std::unique_ptr<TXEndpoint>> _endpoints;

    void _makeHeartbeatTimer() {
        _heartbeatTimer.setCallback([this] {
            K2LOG_D(log::echo, "Sending new request");
            Echo_Message msg{.message = "Hello World!"};
            return seastar::parallel_for_each(_endpoints.begin(), _endpoints.end(),
            [this, &msg] (auto& endp) {
                auto start = k2::Clock::now();
                return RPC().callRPC<Echo_Message, Echo_Message>(MessageVerbs::MES, msg, *endp, 1s)
                .then([&endp, start] (auto&& resp) {
                    auto& [status, msg] = resp;
                    K2LOG_D(log::echo, "Received MSG response with status {}, value {}, from {}, in {}us", status, msg, endp->url, k2::usec(k2::Clock::now() - start));
                });
            }).discard_result();
        });
    }
public:
    seastar::future<> gracefulStop() {
        K2LOG_I(log::echo, "stop");
        return seastar::make_ready_future<>();
    }

    void start() {
        for (auto& ep: _strEndpoints()) {
            _endpoints.push_back(k2::RPC().getTXEndpoint(ep));
        }
        _makeHeartbeatTimer();
        _heartbeatTimer.armPeriodic(_interval());
    }
};
}  // namespace k2

int main(int argc, char** argv) {
    k2::App app("RPCEchoDemo");
    app.addOptions()
    ("interval", bpo::value<k2::ParseableDuration>(), "Ping interval (1s default)")
    ("endpoints", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A space-delimited list of k2 endpoints to contact");
    app.addApplet<k2::PingService>();
    app.addApplet<k2::PingClient>();
    return app.start(argc, argv);
}
