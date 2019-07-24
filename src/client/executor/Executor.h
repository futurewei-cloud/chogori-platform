#pragma once

// std
#include <chrono>
#include <thread>
#include <future>
#include <mutex>
#include <condition_variable>
// boost
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
// seastar
#include <seastar/core/sleep.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics.hh>
// k2
#include <common/Payload.h>
#include <node/NodePool.h>
// k2:transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/RRDMARPCProtocol.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"
#include "transport/Prometheus.h"
// k2:client
#include <client/IClient.h>
#include "TransportPlatform.h"
#include "ExecutorTask.h"

namespace k2
{

//
// Executor service for asynchronous communication with a K2 node.
//
// For every request, the executor will create a task which contains a callback to the client and the payload destined for a K2 node. These tasks are put into
// a queue which the executor periodically polls. The task is considered complete when the K2 request is performed and the client callback is invoked.
//
// The Executor operates in two modes:
// - User initialized thread: in this mode, the transport platform is executing in the client's thread.
// - Thread pool: in this mode, the transport platform is running in a separate thread.
//
// The thread pool mode can get a bit tricky since states between threads have to be shared and the memory management can be different from that of the transport platform.
// To make this easier, the following conventions have been established:
// - The states have been separated into client thread responsibility and transport platform responsibility.
// - Memory allocated in the client thread is dealocated by the client thread.
// - Memory allocated in the transport platform is deallocated by the transport platform.
// - Shared states are thread-safe.
//

namespace metrics = seastar::metrics;

class Executor
{
private:
    // shared
    std::atomic<bool> _initFlag = false;
    std::atomic<bool> _stopFlag = false;
    std::atomic<bool> _userInitThread = false;
    ExecutorQueue _queue;
    // this class
    const uint16_t prometheusTcpPort = 8089;
    std::thread _transportThread;
    TransportPlatform::Dist_t _transport;
    std::vector<const char *> _argv;
    // the mutex is used to wait until the transport platform is started
    std::mutex _mutex;
    std::condition_variable _conditional;
    // from arguments
    client::ClientSettings _settings;
    client::IClient& _rClient;
    k2::Prometheus _prometheus;

public:
    Executor(client::IClient& rClient)
    : _rClient(rClient)
    {
        _argv.push_back("k2-client-executor");
        _argv.push_back("--poll-mode");
    }

    ~Executor()
    {
        stop();
    }

    //
    // Initialize the executor.
    //
    void init(const client::ClientSettings& settings)
    {
        ASSERT(!_initFlag);

        _settings = settings;
        _userInitThread = _settings.userInitThread;
        _stopFlag = false;
        _initFlag = true;

        _argv.push_back("-c");
	    _argv.push_back(std::to_string(_settings.networkThreadCount).c_str());
    }

    //
    // Start the executor.
    //
    void start()
    {
        ASSERT(_initFlag);
        ASSERT(!_stopFlag);

        // if client initialized, run the transport platform in the client's thread
        if(_userInitThread) {
            runTransport();
        }
        else {
             // create separate threads to run the Seastar platform
            _transportThread = std::thread([this] {
                runTransport();
            });
        }


        // wait for the transport to be initialized
        std::unique_lock<std::mutex> lock(_mutex);
        int counter = 5;
        _conditional.wait_for(lock, std::chrono::seconds(1), [&counter] {
            counter--;
            if(counter <= 0) {
                throw std::runtime_error("Executor: failed while waiting for the transport to start!");
            }

            return false;
        });
    }

    //
    // Stop the executor.
    //
    void stop()
    {
         K2INFO("Stopping executor...");

        _stopFlag = true;

        if(!_userInitThread) {
            if(_transportThread.joinable()) {
                _transportThread.join();
            }
        }

        K2INFO("Executor stoped!");
    }

    //
    // Creates payload and sends it to the destination url.
    //
    // throws: runtime_error if the executor's pipeline is full.
    //
    void execute(String url, Duration timeout, PayloadRefCallback fPayloadRef, ResponseCallback fResponse) {
        auto pClientData = std::make_unique<ExecutorTask::ClientData>(std::move(url), timeout, fPayloadRef, fResponse);
        scheduleRequest(std::move(pClientData));
    }

    //
    // Creates payload for the destination url.
    //
    // throws: runtime_error if the executor's pipeline is full.
    //
    void execute(String url, PayloadPtrCallback fPayloadPtr) {
        auto pClientData = std::make_unique<ExecutorTask::ClientData>(std::move(url), fPayloadPtr);
        scheduleRequest(std::move(pClientData));
    }

    //
    // Send the provided payload to the destination url.
    //
    // throws: runtime_error if the executor's pipeline is full.
    //
    void execute(String url, Duration timeout, std::unique_ptr<Payload> pPayload, ResponseCallback fResponse) {
        auto pClientData = std::make_unique<ExecutorTask::ClientData>(std::move(url), std::move(pPayload), timeout, fResponse);
        scheduleRequest(std::move(pClientData));
    }

private:
     void scheduleRequest(std::unique_ptr<ExecutorTask::ClientData> pClientData)
     {
        ASSERT(_initFlag);
        ASSERT(!_stopFlag);

        if(!_queue.push(std::move(pClientData))) {
            throw std::runtime_error("Executor: busy; retry");
        }
    }

    int runTransport() {
        namespace bpo = boost::program_options;

        k2::VirtualNetworkStack::Dist_t virtualNetwork;
        k2::RPCProtocolFactory::Dist_t tcpproto;
	    k2::RPCProtocolFactory::Dist_t rdmaproto;
        k2::RPCDispatcher::Dist_t dispatcher;
	    bool startRdmaFlag = false;

        // TODO: update to use n cores
	    //_argv.push_back("--cpuset");
	    //_argv.push_back("30");
	    //_argv.push_back("--rdma");
	    //_argv.push_back("mlx5_0");
	    //_argv.push_back("-m");
	    //_argv.push_back("10G");
	    //_argv.push_back("--hugepages");

        std::string argString;
        for(const char* pArg : _argv) {
            argString += pArg;
            argString += " ";
            if(std::string(pArg).find("rdma")!=std::string::npos) {
                startRdmaFlag = true;
            }
        }
        _argv.push_back(nullptr);

        K2INFO("Command arguments: " << argString);

        seastar::app_template app;
        int result = app.run_deprecated(_argv.size()-1, (char**)_argv.data(), [&] {
            seastar::engine().at_exit([&] {
                K2INFO("Stopping transport platform...");

                return _transport.stop()
                    .then([&] {
                        K2INFO("Stopping dispatcher...");

                        return dispatcher.stop();
                    })
                    .then([&] {
                        K2INFO("Stopping rdma...");

                        return (startRdmaFlag) ? rdmaproto.stop() : seastar::make_ready_future<>();
                    })
                    .then([&] {
                        K2INFO("Stopping tcp...");

                        return tcpproto.stop();
                    })
                    .then([&] {
                        K2INFO("Stopping vnet...");

                        return virtualNetwork.stop();
                    }).then([&] {
                        K2INFO("Stopping prometheus...");

                        return _prometheus.stop();
                    });
            });

            // poll the stop flag to stop the transport
            seastar::do_until([&] { return _stopFlag.load() && _queue.empty(); }, [&] {
                return seastar::sleep(std::chrono::milliseconds(1));
            })
            .then([&] {
                return _transport.stop();
            });

            K2INFO("Starting transport platform...");

            return _prometheus.start(prometheusTcpPort, "K2 client executor metrics", "k2_client_executor")
                .then([&] {

                    return virtualNetwork.start();
                })
                .then([&] {
                    return tcpproto.start(k2::TCPRPCProtocol::builder(std::ref(virtualNetwork)));
                })
		        .then([&]() {
		            return (startRdmaFlag) ? rdmaproto.start(k2::RRDMARPCProtocol::builder(std::ref(virtualNetwork))) : seastar::make_ready_future<>();
		        })
                .then([&] {

                    return dispatcher.start();
                })
                .then([&] {
                    return _transport.start(
                        TransportPlatform::Settings(_userInitThread.load(), _settings.runInLoop, _rClient),
                        std::ref(_queue),
                        std::ref(dispatcher));
                })
                .then([&] {
                    K2INFO("Starting vnet...");

                    return virtualNetwork.invoke_on_all(&k2::VirtualNetworkStack::start);
                })
                .then([&] {
                    K2INFO("Starting tcp...");

                    return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&] {
                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
                })
		        .then([&] {
		            K2INFO("Starting rdma...");

		            return (startRdmaFlag) ? rdmaproto.invoke_on_all(&k2::RPCProtocolFactory::start) : seastar::make_ready_future<>();
		        })
		        .then([&] {
		            K2INFO("Registering rdma protocol...");

		            return (startRdmaFlag) ? dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(rdmaproto)) : seastar::make_ready_future<>();
		        })
                .then([&] {
                    K2INFO("Starting dispatcher...");

                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
                })
                .then([&] {
                    auto ret = _transport.invoke_on_all(&TransportPlatform::start);
                    K2INFO("Transport started!");

                    return ret;
                })
                .then([&] {
                    // unblock the conditional waiting for the transport to start
                    _conditional.notify_all();

                    return seastar::make_ready_future<>();
                });
        });

        return result;
    }

}; // class Executor

}; // namespace k2
