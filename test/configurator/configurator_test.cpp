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
                        K2LOG_I(log::configuratorclient, "putting record");
                        SET_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return RPC().callRPC<SET_ConfiguratorRequest, SET_ConfiguratorResponse>(CONFGURATOR_SET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "put response : {} {}", status, response );
                        });
                    }).then([this] { 
                        GET_ConfiguratorRequest request{.key=_key()};
                        return RPC().callRPC<GET_ConfiguratorRequest, GET_ConfiguratorResponse>(CONFGURATOR_GET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "got response : {} {}", status, response );
                            }); 
                    }).then([this] { 
                        K2LOG_I(log::configuratorclient, "deleting record");
                        DELETE_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return RPC().callRPC<DELETE_ConfiguratorRequest, DELETE_ConfiguratorResponse>(CONFGURATOR_SET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratorclient, "put response : {} {}", status, response );
                        }); 
                    })
                    auto ep = RPC().getServerEndpoint(TCPRPCProtocol::proto);
                    K2LOG_I(log::configuratorclient, "endpoint: {}", ep->url);
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
};
}

int main(int argc, char** argv) {
    k2::App app("ConfiguratorClient");

    app.addApplet<k2::ConfiguratorClient>();
    return app.start(argc, argv);
}