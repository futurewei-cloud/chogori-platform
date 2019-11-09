#pragma once

// std
#include <thread>
#include <future>
#include <mutex>
#include <condition_variable>
// seastar
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics.hh>
// k2
#include <common/Payload.h>
// k2:transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/RRDMARPCProtocol.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"
#include "transport/Prometheus.h"
// k2:service
#include "IService.h"

namespace k2
{

class ServicePlatform
{
public:
    struct Settings
    {
        bool _useUserThread = false;
        int _threadPoolCount = 1;
        uint16_t _serviceTcpPort = 0;  // dissabled
    };

private:
     // shared
    std::atomic<bool> _initFlag = false;
    std::atomic<bool> _stopFlag = false;
    std::atomic<bool> _useUserThread = false;
    // this class
    const uint16_t _prometheusTcpPort = 8090;
    std::thread _transportThread;
    k2::Prometheus _prometheus;
    std::vector<const char *> _argv;
    std::vector<char> _argVector;
    // the mutex is used to wait until the transport platform is started
    std::mutex _mutex;
    std::condition_variable _conditional;
    // from arguments
    Settings _settings;
    IService& _rService;

public:
    ServicePlatform(IService& rService) :
    _rService(rService)
    {
        _argVector.reserve(100);
        addArgument("k2-service-platform");
        addArgument("--poll-mode");
    }

    ~ServicePlatform()
    {
        stop();
    }

    void init(const Settings& settings)
    {
        ASSERT(!_initFlag);

        _settings = settings;
        _useUserThread = _settings._useUserThread;
        _stopFlag = false;

        addArgument("-c");
	    addArgument(std::to_string(_settings._threadPoolCount));

        _initFlag = true;
    }

    void init(const Settings& settings, const std::vector<const char *>& args)
    {
        for(const char* pArg : args) {
            addArgument(pArg);
        }

        init(settings);
    }

    void start()
    {
        ASSERT(_initFlag);
        ASSERT(!_stopFlag);

        // if client initialized, run the transport platform in the client's thread
        if(_useUserThread) {
            K2INFO("Starting service platform in the user thread");
            runService();
        }
        else {
             // create separate threads to run the Seastar platform
            _transportThread = std::thread([this] {
                K2INFO("Starting service platform in separate thread");
                runService();
            });
        }

        // wait for the service to start before returning
        std::unique_lock<std::mutex> lock(_mutex);
        int counter = 5;
        _conditional.wait_for(lock, std::chrono::milliseconds(1), [&counter] {
            counter--;

            if(counter <= 0) {

                throw std::runtime_error("Platform: failed while waiting for the transport to start!");
            }

            return false;
        });
    }

    void stop()
    {
         K2INFO("Stopping platform...");

        _stopFlag = true;

        if(!_useUserThread) {
            if(_transportThread.joinable()) {
                _transportThread.join();
            }
        }

        K2INFO("Platform stopped!");
    }

protected:
    void addArgument(const std::string& arg) {
        int index = _argVector.size();
        for(char c : arg) {
            _argVector.emplace_back(c);
        }
        _argVector.emplace_back('\0');
        const char* argc = &_argVector[index];
        _argv.emplace_back(argc);
    }

    int runService() {
        namespace bpo = boost::program_options;

        k2::VirtualNetworkStack::Dist_t virtualNetwork;
        k2::RPCProtocolFactory::Dist_t tcpproto;
	    k2::RPCProtocolFactory::Dist_t rdmaproto;
        k2::RPCDispatcher::Dist_t dispatcher;
	    bool startRdmaFlag = false;

        // check if rdma is enabled
        std::string argString;
        for(const char* pArg : _argv) {
            argString += pArg;
            argString += " ";
            if(std::string(pArg).find("rdma") != std::string::npos) {
                startRdmaFlag = true;
            }
        }
        _argv.push_back(nullptr);

        K2INFO("Command arguments: " << argString);

        seastar::app_template app;
        int result = app.run_deprecated(_argv.size()-1, (char**)_argv.data(), [&] {
            seastar::engine().at_exit([&] {
                K2INFO("Stopping service...");

                return _rService.stop()
                    .then([&] {
                        K2INFO("Stopping dispatcher...");

                        return dispatcher.stop();
                    })
                    .then([&] {
                        if(startRdmaFlag) {
                            K2INFO("Stopping rdma...");

                            return rdmaproto.stop();
                        }
                        else {
                            return seastar::make_ready_future<>();
                        }
                    })
                    .then([&] {
                        K2INFO("Stopping tcp...");

                        return tcpproto.stop();
                    })
                    .then([&] {
                        K2INFO("Stopping vnet...");

                        return virtualNetwork.stop();
                    }).then([&] {
                        if(_prometheusTcpPort > 0) {
                            K2INFO("Stopping prometheus...");

                            return _prometheus.stop();
                        }
                        else {

                            return seastar::make_ready_future<>();
                        }
                    });
            });

            std::vector<seastar::future<>> futures;

            // keep polling the stop flag to stop the service
            auto future1 = seastar::do_until([&] { return _stopFlag.load(); }, [&] {
                return seastar::sleep(std::chrono::seconds(1));
            })
            .then([&] {
                return _rService.stop().then([&] {
                    // at times it is required to hit ctrl-c to stop the engine; explicitly stop the engine to prevent this
                    seastar::engine().exit(0);
                });
            }).or_terminate();

            futures.push_back(std::move(future1));

            K2INFO("Starting service platform...");
            auto future2 = seastar::make_ready_future<>()
                .then([&] {
                    if(_prometheusTcpPort > 0) {
                        return _prometheus.start(_prometheusTcpPort, "K2 service platform metrics", "k2_service_platform");
                    }
                    else {
                        return seastar::make_ready_future<>();
                    }
                }).then([&] {
                   return virtualNetwork.start();
                })
                .then([&] {
                    RPCProtocolFactory::BuilderFunc_t builder =  k2::TCPRPCProtocol::builder(std::ref(virtualNetwork));
                    if(_settings._serviceTcpPort > 0 ) {
                        builder = k2::TCPRPCProtocol::builder(std::ref(virtualNetwork), _settings._serviceTcpPort);
                    }

                    return tcpproto.start(builder);
                })
		        .then([&]() {
		            return (startRdmaFlag) ? rdmaproto.start(k2::RRDMARPCProtocol::builder(std::ref(virtualNetwork))) : seastar::make_ready_future<>();
		        })
                .then([&] {

                    return dispatcher.start();
                })
                .then([&] {
                    return _rService.init(std::ref(dispatcher));
                })
                .then([&] {
                    K2INFO("Starting vnet...");

                    return virtualNetwork.invoke_on_all(&k2::VirtualNetworkStack::start);
                })
                .then([&] {
                    K2INFO("Starting tcp factory...");

                    return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&] {
                    K2INFO("Starting tcp protocol...");

                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
                })
		        .then([&] {
		            if(startRdmaFlag) {
                        K2INFO("Starting rdma factory...");
                        return rdmaproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                    }
                    else {
                        return seastar::make_ready_future<>();
                    }
		        })
		        .then([&] {
                    if(startRdmaFlag) {
		                K2INFO("Starting rdma protocol...");

                        return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(rdmaproto));
                    }
                    else {
		                return seastar::make_ready_future<>();
                    }
		        })
                .then([&] {
                    K2INFO("Starting dispatcher...");

                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
                })
                .then([&] {
                    K2INFO("Starting service...");

                    auto ret = _rService.start();
                     // unblock the conditional waiting for the transport to start
                    _conditional.notify_all();

                    return ret;
                }).or_terminate();

            futures.push_back(std::move(future2));

            return seastar::when_all_succeed(futures.begin(), futures.end())
                .handle_exception([] (std::exception_ptr eptr) {
                    K2ERROR("Service platform: exception: " << eptr);

                    return seastar::make_ready_future<>();
                })
                .finally([&] {
                    K2INFO("Stopping seastar engine...");
                    // at times it is required to hit ctrl-c to stop the engine; explicitly stop the engine to prevent this
                    seastar::engine().exit(0);

                    return seastar::make_ready_future<>();
                }).or_terminate();
        });

        return result;
    }
}; // ServicePlatform

}; // namespace k2
