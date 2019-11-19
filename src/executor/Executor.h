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
#include <seastar/core/semaphore.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics.hh>
// k2
#include <common/Payload.h>
#include <node/NodePool.h>
// k2:config
#include <config/Config.h>
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
// K2:executor
#include "ServicePlatform.h"
#include "MessageService.h"
#include "ApplicationService.h"

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
    // this class
    ServicePlatform _platform;
    // shared
    std::vector<std::unique_ptr<ExecutorQueue>> _queues;
    std::vector<std::unique_ptr<IServiceLauncher>> _launchers;
    std::vector<const char *> _argv;
    // from arguments
    client::ClientSettings _settings;
public:
    Executor(client::IClient& rClient)
    {
        (void)rClient;
        // empty
    }

    Executor()
    {
        // empty
    }

    ~Executor()
    {
        stop();
    }

    void init(const client::ClientSettings& settings);
    void init(const client::ClientSettings& settings, std::shared_ptr<config::NodePoolConfig> pNodePoolConfig);

    template<typename T>
    void registerApplication(IApplication<T>& rApplication, T& rContext)
    {
        auto pLauncher = std::unique_ptr<IServiceLauncher>(new typename ApplicationService<T>::Launcher(rApplication, rContext));
        _platform.registerService(std::ref(*pLauncher));
        _launchers.push_back(std::move(pLauncher));
    }

    //
    // Start the executor.
    //
    void start()
    {
        K2INFO("Starting executor...");
        _platform.start();
    }

    //
    // Stop the executor.
    //
    void stop()
    {
        _platform.stop();
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
        const int index = rand()%(_settings.networkThreadCount);
        if(!_queues[index]->push(std::move(pClientData))) {
            throw std::runtime_error("Executor busy; retry");
        }
    }
}; // class Executor

}; // namespace k2
