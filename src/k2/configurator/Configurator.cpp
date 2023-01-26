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

#include "Configurator.h"
#include "ConfiguratorDTO.h"


#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/logging/Log.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

namespace k2 {

namespace log {
inline thread_local k2::logging::Logger configurator("k2::configurator_server");
}

Configurator::Configurator() {
    K2LOG_I(log::configurator, "ctor");
}

Configurator::~Configurator() {
    K2LOG_I(log::configurator, "dtor");
}

seastar::future<> Configurator::start() {
    K2LOG_I(log::configurator, "Registering message handlers");

    RPC().registerRPCObserver<SET_ConfiguratorRequest, SET_ConfiguratorResponse>(InternalVerbs::CONFGURATOR_SET, [this](SET_ConfiguratorRequest&& request) {
                K2LOG_I(log::configurator, "Received set for key: {}", request.key);
                auto key = request.key;
                auto value = request.value;

                if (request.applyToAll) {
                    return ConfigDist().invoke_on_all([key, value](auto& config) {
                        config.emplace(key, boost::program_options::variable_value(value, ""));
                        boost::program_options::notify(config);    
                    }).then([key] {
                        SET_ConfiguratorResponse response{.key=std::move(key)};
                        return RPCResponse(Statuses::S201_Created("Configurator set accepted"), std::move(response));
                    });
                } else {
                    auto& conf = const_cast<config::BPOVarMap&>(Config());
                    conf.emplace(key, boost::program_options::variable_value(value, ""));
                    boost::program_options::notify(conf);
                    SET_ConfiguratorResponse response{.key=std::move(request.key)};
                    return RPCResponse(Statuses::S201_Created("Configurator set accepted"), std::move(response));
                }
            });

    RPC().registerRPCObserver<GET_ConfiguratorRequest, GET_ConfiguratorResponse>(InternalVerbs::CONFGURATOR_GET, [this](GET_ConfiguratorRequest&& request) {
                K2LOG_I(log::configurator, "Received get for key: {}", request.key);
                auto key = request.key;

                GET_ConfiguratorResponse response;
                response.key=request.key;
                auto iter = Config().find(key);
                if (iter != Config().end()) {    
                    response.value = (iter->second).as<String>();
                } else {
                    return RPCResponse(Statuses::S404_Not_Found("key not found"), std::move(response));
                }
                return RPCResponse(Statuses::S200_OK("get accepted"), std::move(response));
            });

    RPC().registerRPCObserver<DELETE_ConfiguratorRequest, DELETE_ConfiguratorResponse>(InternalVerbs::CONFGURATOR_DELETE, [this](DELETE_ConfiguratorRequest&& request) {
                K2LOG_I(log::configurator, "Received delete for key: {}", request.key);
                 auto& key = request.key;

                if (request.applyToAll) {
                    return ConfigDist().invoke_on_all([key](auto& config) {
                        if (config.count(key)) {
                            config.erase(key);
                        }
                    }).then([key] {
                        DELETE_ConfiguratorResponse response{.key=std::move(key)};
                        return RPCResponse(Statuses::S201_Created("Configurator set accepted"), std::move(response));
                    });
                } else {
                    auto& config = const_cast<config::BPOVarMap&>(Config());
                    if (config.count(key)) {
                        config.erase(key);
                    }
                    DELETE_ConfiguratorResponse response{.key=std::move(request.key)};
                    return RPCResponse(Statuses::S201_Created("Configurator erase accepted"), std::move(response));
                }
            });

    return seastar::make_ready_future();
}

seastar::future<> Configurator::gracefulStop() {
    K2LOG_I(log::configurator, "stop");
    return seastar::make_ready_future<>();
}


} // namespace k2
