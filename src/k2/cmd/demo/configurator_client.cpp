#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/common/Timer.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

#include "k2/configurator/ConfiguratorDTO.h"

namespace k2 {
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
                    auto ep = RPC().getServerEndpoint(TCPRPCProtocol::proto);
                    K2LOG_I(log::configuratorclient, "endpoint: {}", ep->url);
                    if (_op() == "GET") {
                        GET_ConfiguratorRequest request{.key=_key()};
                        return RPC().callRPC<GET_ConfiguratorRequest, GET_ConfiguratorResponse>(CONFGURATOR_GET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "got response : {} {}", status, response );
                            });
                    } else if (_op() == "PUT") {
                        K2LOG_I(log::configuratorclient, "putting record");
                        SET_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return RPC().callRPC<SET_ConfiguratorRequest, SET_ConfiguratorResponse>(CONFGURATOR_SET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "put response : {} {}", status, response );
                        });
                    } else if (_op() == "DELETE") {
                        K2LOG_I(log::configuratorclient, "deleting record");
                        DELETE_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return RPC().callRPC<DELETE_ConfiguratorRequest, DELETE_ConfiguratorResponse>(CONFGURATOR_SET, request, *ep, 1s)
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
                    AppBase().stop(0);
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
        SingleTimer _singleTimer;
        ConfigVar<String> _key{"key"};
        ConfigVar<String> _value{"value"};
        ConfigVar<String> _op{"op"};
        ConfigVar<bool> _applyToAll{"applyToAll"};
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

    app.addApplet<k2::ConfiguratorClient>();
    return app.start(argc, argv);
}
