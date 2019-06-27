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
// k2
#include <common/Payload.h>
#include <node/NodePool.h>
// k2:transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"
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

class Executor
{
private:
    // shared
    std::atomic<bool> _initFlag = false;
    std::atomic<bool> _stopFlag = false;
    std::atomic<bool> _userInitThread = false;
    ExecutorQueue _queue;
    // this class
    std::thread _transportThread;
    TransportPlatform::Dist_t _transport;
    // the mutex is used to wait until the transport platform is started
    std::mutex _mutex;
    std::condition_variable _conditional;
    // from arguments
    client::ClientSettings _settings;
    client::IClient& _rClient;

public:
    Executor(client::IClient& rClient)
    : _rClient(rClient)
    {
        // empty
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
        k2::RPCProtocolFactory::Dist_t protocolFactory;
        k2::RPCDispatcher::Dist_t dispatcher;

        seastar::app_template app;

        // TODO: update to use n cores
        char *argv[] = {(char *)"k2-seastar-executor", (char*)"-c1", nullptr};
        int argc = sizeof(argv) / sizeof(*argv) - 1;

        int result = app.run(argc, argv, [&] {
            seastar::engine().at_exit([&] {
                K2INFO("Stopping transport platform...");

                return _transport.stop()
                    .then([&] {
                        K2INFO("Stopping dispatcher...");

                        return dispatcher.stop();
                    })
                    .then([&] {
                        K2INFO("Stopping tcp...");

                        return protocolFactory.stop();
                    })
                    .then([&] {
                      K2INFO("Stopping vnet...");

                      return virtualNetwork.stop();
                    });
            });

            // poll the stop flag to stop the transport
            seastar::do_until([&] { return _stopFlag.load() && _queue.empty(); }, [&] {
                return seastar::sleep(std::chrono::seconds(1));
            })
            .then([&] {
                return _transport.stop();
            });

            K2INFO("Starting transport platform...");

            return virtualNetwork.start()
                .then([&] {
                    return protocolFactory.start(k2::TCPRPCProtocol::builder(std::ref(virtualNetwork), 0));
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

                    return protocolFactory.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&] {
                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(protocolFactory));
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
