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

#include <seastar/core/future.hh>

#include <k2/config/Config.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/logging/Log.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

namespace k3 {

namespace log {
inline thread_local k2::logging::Logger configurator("k2::configurator_server");
}
class Configurator {
public :
    Configurator();
    ~Configurator();

    seastar::future<> gracefulStop();
    seastar::future<> start();


private :
    k2::ConfigVar<k2::String> _key{"key"};
    k2::ConfigVar<k2::String> _value{"value"};
    k2::ConfigVar<k2::String> _op{"op"};
    k2::ConfigVar<k2::String> _applyToAll{"applyToAll"}; 
};  // class Configurator

struct DELETE_ConfiguratorRequest {
    k2::String key;
    k2::String value;
    bool applyToAll;
    K2_PAYLOAD_FIELDS(key, value, applyToAll);
    K2_DEF_FMT(DELETE_ConfiguratorRequest, key, value, applyToAll);
};

struct DELETE_ConfiguratorResponse {
    k2::String key;
    K2_PAYLOAD_FIELDS(key);
    K2_DEF_FMT(DELETE_ConfiguratorResponse, key);
};

struct SET_ConfiguratorRequest {
    k2::String key;
    k2::String value;
    bool applyToAll;
    K2_PAYLOAD_FIELDS(key, value, applyToAll);
    K2_DEF_FMT(SET_ConfiguratorRequest, key, value, applyToAll);
};

struct SET_ConfiguratorResponse {
    k2::String key;
    K2_PAYLOAD_FIELDS(key);
    K2_DEF_FMT(SET_ConfiguratorResponse, key);
};

struct GET_ConfiguratorRequest {
    k2::String key;
    K2_PAYLOAD_FIELDS(key);
    K2_DEF_FMT(GET_ConfiguratorRequest, key);
};

struct GET_ConfiguratorResponse {
    k2::String key;
    k2::String value;
    K2_PAYLOAD_FIELDS(key, value);
    K2_DEF_FMT(GET_ConfiguratorResponse, key, value);
};

Configurator::Configurator() {
    K2LOG_I(log::configurator, "ctor");
}

Configurator::~Configurator() {
    K2LOG_I(log::configurator, "dtor");
}

seastar::future<> Configurator::start() {
    K2LOG_I(log::configurator, "Registering message handlers");

    k2::RPC().registerRPCObserver<SET_ConfiguratorRequest, SET_ConfiguratorResponse>(k2::InternalVerbs::CONFGURATOR_SET, [this](SET_ConfiguratorRequest&& request) {
                K2LOG_I(log::configurator, "Received set for key: {}", request.key);
                auto key = request.key;
                auto value = request.value;

                if (request.applyToAll) {
                    return k2::ConfigDist().invoke_on_all([key, value](auto& config) {
                        config.emplace(key, boost::program_options::variable_value(value, ""));
                        boost::program_options::notify(config);    
                    }).then([key] {
                        SET_ConfiguratorResponse response{.key=std::move(key)};
                        return k2::RPCResponse(k2::Statuses::S201_Created("Configurator set accepted"), std::move(response));
                    });
                } else {
                    auto& conf = const_cast<k2::config::BPOVarMap&>(k2::Config());
                    conf.emplace(key, boost::program_options::variable_value(value, ""));
                    boost::program_options::notify(conf);
                    SET_ConfiguratorResponse response{.key=std::move(request.key)};
                    return k2::RPCResponse(k2::Statuses::S201_Created("Configurator set accepted"), std::move(response));
                }
            });

    k2::RPC().registerRPCObserver<GET_ConfiguratorRequest, GET_ConfiguratorResponse>(k2::InternalVerbs::CONFGURATOR_GET, [this](GET_ConfiguratorRequest&& request) {
                K2LOG_I(log::configurator, "Received get for key: {}", request.key);
                auto key = request.key;

                GET_ConfiguratorResponse response;
                response.key=request.key;
                auto iter = k2::Config().find(key);
                if (iter != k2::Config().end()) {    
                    response.value = (iter->second).as<k2::String>();
                } else {
                    return k2::RPCResponse(k2::Statuses::S404_Not_Found("key not found"), std::move(response));
                }
                return k2::RPCResponse(k2::Statuses::S200_OK("get accepted"), std::move(response));
            });

    k2::RPC().registerRPCObserver<DELETE_ConfiguratorRequest, DELETE_ConfiguratorResponse>(k2::InternalVerbs::CONFGURATOR_DELETE, [this](DELETE_ConfiguratorRequest&& request) {
                K2LOG_I(log::configurator, "Received delete for key: {}", request.key);
                 auto& key = request.key;

                if (request.applyToAll) {
                    return k2::ConfigDist().invoke_on_all([key](auto& config) {
                        if (config.count(key)) {
                            config.erase(key);
                        }
                    }).then([key] {
                        DELETE_ConfiguratorResponse response{.key=std::move(key)};
                        return k2::RPCResponse(k2::Statuses::S201_Created("Configurator set accepted"), std::move(response));
                    });
                } else {
                    auto& config = const_cast<k2::config::BPOVarMap&>(k2::Config());
                    if (config.count(key)) {
                        config.erase(key);
                    }
                    DELETE_ConfiguratorResponse response{.key=std::move(request.key)};
                    return k2::RPCResponse(k2::Statuses::S201_Created("Configurator erase accepted"), std::move(response));
                }
            });

    return seastar::make_ready_future();
}

seastar::future<> Configurator::gracefulStop() {
    K2LOG_I(log::configurator, "stop");
    return seastar::make_ready_future<>();
}


} // namespace k3

int main(int argc, char** argv) {
    k2::App app("Configurator");
    app.addApplet<k3::Configurator>();
    return app.start(argc, argv);
}