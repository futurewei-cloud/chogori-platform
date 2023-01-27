#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/common/Timer.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

#include "k2/configurator/ConfiguratorDTO.h"

namespace k3 {
namespace log {
inline thread_local k2::logging::Logger configuratorclient("k2::configurator_client");
}

class ConfiguratorClient {
    public:
        seastar::future<> start() {

            K2LOG_I(log::configuratorclient, "Registering message handlers");

            _singleTimer.setCallback([this] {
                return seastar::make_ready_future()
                .then([this]() {
                    auto ep = k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto);
                    K2LOG_I(log::configuratorclient, "endpoint: {}", ep->url);
                    if (_op() == "GET") {
                        k2::GET_ConfiguratorRequest request{.key=_key()};
                        return k2::RPC().callRPC<k2::GET_ConfiguratorRequest, k2::GET_ConfiguratorResponse>(k2::CONFGURATOR_GET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "got response : {} {}", status, response );
                            });
                    } else if (_op() == "PUT") {
                        K2LOG_I(log::configuratorclient, "putting record");
                        k2::SET_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return k2::RPC().callRPC<k2::SET_ConfiguratorRequest, k2::SET_ConfiguratorResponse>(k2::CONFGURATOR_SET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "put response : {} {}", status, response );
                        });
                    } else if (_op() == "DELETE") {
                        K2LOG_I(log::configuratorclient, "deleting record");
                        k2::DELETE_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return k2::RPC().callRPC<k2::DELETE_ConfiguratorRequest, k2::DELETE_ConfiguratorResponse>(k2::CONFGURATOR_SET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "put response : {} {}", status, response );
                        });
                    }
                     else {
                        K2LOG_E(log::configuratorclient, "Not supported operation {}", _op());
                        return seastar::make_ready_future();
                    }
                })
                .then([] {
                    k2::AppBase().stop(0);
                });
            });

            _singleTimer.arm(0s);
            return seastar::make_ready_future();
        }

        seastar::future<> gracefulStop() {
            K2LOG_I(log::configuratorclient, "stop");
            return _singleTimer.stop();
        }

    private :
        k2::SingleTimer _singleTimer;
        k2::ConfigVar<k2::String> _key{"key"};
        k2::ConfigVar<k2::String> _value{"value"};
        k2::ConfigVar<k2::String> _op{"op"};
        k2::ConfigVar<bool> _applyToAll{"applyToAll"};
};
}

int main(int argc, char** argv) {
    k2::App app("ConfiguratorClient");

    app.addOptions()
    ("key", bpo::value<k2::String>(), "e.g. '--key abc");

    app.addOptions()
    ("value", bpo::value<k2::String>(), "e.g. '--value 100");

    app.addOptions()
    ("op", bpo::value<k2::String>(), "e.g. '--op GET, or --op SET");

    app.addOptions()
    ("applyToAll", bpo::value<bool>(), "e.g. '--applyToAll true, or --applyToAll false");

    app.addApplet<k3::ConfiguratorClient>();
    return app.start(argc, argv);
}
