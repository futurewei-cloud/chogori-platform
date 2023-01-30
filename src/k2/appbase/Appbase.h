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

// stl
#include <string>

// third-party
#include <boost/program_options.hpp>
#include <boost/pointer_cast.hpp>
#include <seastar/core/app-template.hh>  // for app_template

// k2 base
#include <k2/common/TypeMap.h>

// k2 transport
#include <k2/transport/AutoRRDMARPCProtocol.h>
#include <k2/transport/Discovery.h>
#include <k2/configurator/Configurator.h>
#include <k2/transport/RPCProtocolFactory.h>
#include <k2/transport/RRDMARPCProtocol.h>
#include <k2/transport/TCPRPCProtocol.h>
#include <k2/transport/VirtualNetworkStack.h>

#include "AppEssentials.h"

namespace k2::log {
inline thread_local k2::logging::Logger appbase("k2::appbase");
}

namespace k2 {

// Helper class used to provide listening addresses for the TCP protocol
class MultiAddressProvider : public k2::IAddressProvider {
   public:
    MultiAddressProvider() = default;
    MultiAddressProvider(const std::vector<String>& urls) : _urls(urls) {}
    MultiAddressProvider& operator=(MultiAddressProvider&&) = default;
    seastar::socket_address getAddress(int coreID) const override {
        if (size_t(coreID) < _urls.size()) {
            K2LOG_D(log::appbase, "On core {} have url {}", coreID, _urls[coreID]);
            auto ep = k2::TXEndpoint::fromURL(_urls[coreID], BinaryAllocator());
            if (ep) {
                return seastar::socket_address(seastar::ipv4_addr(ep->ip, uint16_t(ep->port)));
            }
            // might not be in URL form (e.g. just a plain port)
            K2LOG_D(log::appbase, "attempting to use url as a simple port");
            try {
                auto port = std::stoi(_urls[coreID]);
                return seastar::socket_address((uint16_t)port);
            } catch (...) {
            }
            K2ASSERT(log::appbase, false, "Unable to construct Endpoint from URL: {}", _urls[coreID]);
        }
        K2LOG_I(log::appbase, "This core does not have a port assignment: {}", coreID);
        return seastar::socket_address(seastar::ipv4_addr{0});
    }

   private:
    std::vector<String> _urls;
};  // class MultiAddressProvider

// This is a foundational class used to create K2 Apps.
class App {
public:  // API
    App(String name):_name(name){
        // Name is used as a metrics prefix and cannot contain spaces or other weird chars
        for (size_t i = 0; i < _name.size(); ++i) {
            if (!std::isalnum(_name[i])) {
                _name[i] = '_';
            }
        }
        // add the discovery applet o all apps
        addApplet<k2::Discovery>();
        addApplet<k2::Configurator>();
    }

    // helper class for positional option adding
    class PosOptAdder {
       public:
        PosOptAdder(App* app) : _app(app) {}
        PosOptAdder& operator()(const char* name, const bpo::value_semantic* value_semantic, const char* help, int max_count) {
            _app->_app.add_positional_options({{name, value_semantic, help, max_count}});
            return *this;
        }

       private:
        App* _app;
    };
    // Use this method to obtain a callable which can be used to add additional
    // command-line options (see how we use it below)
    bpo::options_description_easy_init addOptions() { return _app.add_options(); }

    // Use this method to add positional options. The method returns an option applier much like getOptions() above
    PosOptAdder addPositionalOptions() {
        return PosOptAdder(this);
    }

    // This method returns the distributed container for the AppletType.
    // This container can then be used to perform map/reduce type operations (see ss::distributed API)
    template <typename AppletType>
    seastar::distributed<AppletType>& getDist() {
        auto findIter = _applets.find<AppletType>();
        if (findIter == _applets.end()) {
            throw std::runtime_error("applet not found");
        }
        // this is safe, since we created the object ourselves, based on AppletType
        // in other words, if we found an entry of type AppletType, then the value stored in the map
        // is guaranteed to be of type seastar::distributed<AppletType>*
        return *(static_cast<seastar::distributed<AppletType>*>(findIter->second));
    }

    // Add a applet to the app
    template <typename AppletType, typename... ConstructorArgs>
    void addApplet(ConstructorArgs&&... ctorArgs) {
        if (_applets.find<AppletType>() != _applets.end()) {
            throw std::runtime_error("duplicate applets not allowed");
        }

        seastar::distributed<AppletType>* dd = new seastar::distributed<AppletType>();
        _ctors.push_back(
            [dd, args = std::make_tuple(std::forward<ConstructorArgs>(ctorArgs)...)]() mutable {
                // Start the distributed container which will construct the objects on each core
                return std::apply(
                    [dd](ConstructorArgs&&... args) {
                        return dd->start(std::forward<ConstructorArgs>(args)...);
                    },
                    std::move(args));
            });
        _starters.push_back([dd]() mutable { return dd->invoke_on_all(&AppletType::start); });
        _gracefulStoppers.push_back([dd]() mutable { return dd->invoke_on_all(&AppletType::gracefulStop); });
        _stoppers.push_back([dd]() mutable { return dd->stop(); });
        _dtors.push_back([dd]() mutable { delete dd; });

        // type-erase the container and put it in the map.
        _applets.put<AppletType>((void*)dd);
    }

    // This method should be called to initialize the system.
    // 1. All applets are constructed with the arguments supplied when addApplet() was called
    // Once all components are started, we call AppletTypes::start() to let the user begin their workflow
    int start(int argc, char** argv);

    // should be called by applets if they want to exit the app. The return code will be the return code from main()
    void stop(int retcode);

    ~App() {
        for (auto rit = _dtors.rbegin(); rit != _dtors.rend(); ++rit) {
            (*rit)();
        }
    }

private:
    String _name;
    seastar::app_template _app;
    TypeMap<void*> _applets;
    std::vector<std::function<seastar::future<>()>> _ctors;     // functors which create user applets
    std::vector<std::function<seastar::future<>()>> _starters;  // functors which call start() on user applets
    std::vector<std::function<seastar::future<>()>> _gracefulStoppers;  // functors which call gracefulStop() on user applets
    std::vector<std::function<seastar::future<>()>> _stoppers;  // functors which call stop() on user applets and delete them
    std::vector<std::function<void()>> _dtors;                  // functors which delete the distributed containers
};                                                              // class App

// global access to the AppBase
inline App* ___appBase___;
inline App& AppBase() { return *___appBase___; }
} // namespace k2
