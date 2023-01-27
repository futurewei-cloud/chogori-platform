#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/common/Timer.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

#include "k2/configurator/ConfiguratorDTO.h"

namespace k2 {
namespace log {
inline thread_local k2::logging::Logger configuratortest("k2::configuratortest");
}

class ConfiguratorTest {
    public:
        seastar::future<> start() {

            K2LOG_I(log::configuratortest, "Registering message handlers");
            auto ep = RPC().getServerEndpoint(TCPRPCProtocol::proto);

            _singleTimer.setCallback([ep, this] {
                return seastar::make_ready_future()
                .then([ep, this]() {
                    K2LOG_I(log::configuratortest, "endpoint: {}", ep->url);
                        K2LOG_I(log::configuratortest, "putting record");
                        SET_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return RPC().callRPC<SET_ConfiguratorRequest, SET_ConfiguratorResponse>(CONFGURATOR_SET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratortest, "put response : {} {}", status, response );
                        });
                    }).then([ep, this] { 
                        GET_ConfiguratorRequest request{.key=_key()};
                        return RPC().callRPC<GET_ConfiguratorRequest, GET_ConfiguratorResponse>(CONFGURATOR_GET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratortest, "got response : {} {}", status, response );
                            }); 
                    }).then([ep, this] { 
                        K2LOG_I(log::configuratortest, "deleting record");
                        DELETE_ConfiguratorRequest request{.key=_key(), .value=_value(), .applyToAll=_applyToAll()};
                        return RPC().callRPC<DELETE_ConfiguratorRequest, DELETE_ConfiguratorResponse>(CONFGURATOR_SET, request, *ep, 1s)
                        .then([](auto&& result) {
                            auto& [status, response] = result;
                            K2LOG_I(log::configuratortest, "put response : {} {}", status, response );
                        }); 
                    })
                    .then([] {
                        AppBase().stop(0);
                    });
                });

                _singleTimer.arm(0s);
                return seastar::make_ready_future();
        }

        seastar::future<> gracefulStop() {
            K2LOG_I(log::configuratortest, "stop");
            return _singleTimer.stop();
        }

    private :
        SingleTimer _singleTimer;
        ConfigVar<String> _key{"key"};
        k2::ConfigVar<k2::String> _value{"value"};
        k2::ConfigVar<bool> _applyToAll{"applyToAll"};
 };
}

int main(int argc, char** argv) {
    k2::App app("ConfiguratorTest");
    app.addOptions()
    ("key", bpo::value<k2::String>(), "e.g. '--key abc");
    app.addOptions()
    ("value", bpo::value<k2::String>(), "e.g. '--value 100");
    app.addOptions()
    ("applyToAll", bpo::value<bool>()->default_value(false), "e.g. '--applyToAll true, or --applyToAll false");
    app.addApplet<k2::ConfiguratorTest>();
    return app.start(argc, argv);
}