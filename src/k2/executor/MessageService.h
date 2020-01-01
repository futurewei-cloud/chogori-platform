#pragma once

// std
#include <atomic>
// boost
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
// seastar
#include <seastar/core/reactor.hh>
// k2:transport
#include <k2/k2types/MessageVerbs.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RPCProtocolFactory.h>
#include <k2/transport/RetryStrategy.h>
// k2:client
#include <k2/client/IClient.h>
#include "ExecutorQueue.h"
#include "ExecutorTask.h"
#include "IService.h"

namespace k2
{

namespace metrics = seastar::metrics;

class MessageService: public IService
{
public:
    // Service launcher
    class Launcher: public IServiceLauncher
    {
    private:
        std::unique_ptr<seastar::distributed<MessageService>> _pDistributed;
        std::vector<std::unique_ptr<ExecutorQueue>>& _queues;

    public:
        Launcher(std::vector<std::unique_ptr<ExecutorQueue>>& queues)
        : _queues(queues)
        {
            // EMPTY
        }

        virtual seastar::future<> init(k2::RPCDispatcher::Dist_t& rDispatcher) {
            if(nullptr == _pDistributed.get()) {
                // create the service in the Seastar context
                _pDistributed = std::make_unique<seastar::distributed<MessageService>>();
            }

            return _pDistributed->start(std::ref(rDispatcher), std::ref(_queues));
        }

        virtual seastar::future<> stop() {
            if(nullptr == _pDistributed.get()) {

                return seastar::make_ready_future<>();
            }

            return _pDistributed->stop().then([&] {
                _pDistributed.release();

               return seastar::make_ready_future<>();
            });
        }

        virtual seastar::future<> start() {

           return _pDistributed->invoke_on_all(&MessageService::start);
        }
    };

private:
    class ExecutionException: public std::exception
    {
    protected:
        std::exception_ptr _cause;
        Status _status;
        std::string _message = "Exception thrown during execution.";
    public:
        ExecutionException(Status status)
        : _status(status)
        {
            // empty
        }

        ExecutionException(Status status, std::string message)
        : _status(status)
        , _message(std::move(message))
        {
            // empty
        }

        virtual const char* what() const noexcept override
        {
            return _message.c_str();
        }

        const Status& getStatus() const
        {
            return _status;
        }
    };

    // class defined
    bool _stopFlag = false; // stops the service
    const Duration _minDelay; // the minimum amount of time before calling the next application loop
    TimePoint _runTaskTimepoint;
    seastar::semaphore _semaphore;
    // metrics
    TimePoint _taskLoopScheduleTime;
    TimePoint _clientLoopScheduleTime;
    metrics::metric_groups _metricGroups;
    ExponentialHistogram _sendMessageLatency;
    ExponentialHistogram _createPayloadLatency;
    ExponentialHistogram _createEndpointLatency;
    ExponentialHistogram _taskTime;
    ExponentialHistogram _dequeueTime;
    ExponentialHistogram _clientLoopTime;
    ExponentialHistogram _payloadCallbackTime;
    ExponentialHistogram _resultCallbackTime;
    ExponentialHistogram _taskLoopIdleTime;
    ExponentialHistogram _clientLoopIdleTime;
    // from arguments
    RPCDispatcher::Dist_t& _dispatcher;
    ExecutorQueue& _queue;

public:
    MessageService(
        RPCDispatcher::Dist_t& dispatcher,
        std::vector<std::unique_ptr<ExecutorQueue>>& _queues)
    : _minDelay(5us)
    , _semaphore(seastar::semaphore(1))
    , _dispatcher(dispatcher)
    , _queue(*(_queues[seastar::engine().cpu_id()].get()))
    {
        auto now = Clock::now();
        _runTaskTimepoint = now;
        _taskLoopScheduleTime = now;
        _clientLoopScheduleTime = now;
    }

    virtual seastar::future<> start()
    {
        K2INFO("Starting message service on cpu:"  << seastar::engine().cpu_id());

        registerMetrics();

        // start message polling
        return seastar::with_semaphore(_semaphore, 1, [&] {
            return seastar::do_until([&] { return _stopFlag && _queue.empty(); }, [&] {
                const auto now = Clock::now();
                _taskLoopIdleTime.add(now - _taskLoopScheduleTime);
                _taskLoopScheduleTime = now;

                return getNextTask().then([&] (ExecutorTaskPtr pTask) {
                    if(!pTask || !pTask.get()) {

                        return seastar::sleep(10us);
                    }

                    const auto timePoint = Clock::now();
                    _dequeueTime.add(timePoint - pTask->_pClientData->_startTime);
                    pTask->_pPlatformData.reset(new ExecutorTask::PlatformData());

                    // execute the task and invoke the client's callback
                    return executeTask(pTask);
                });
            })
            .handle_exception([] (std::exception_ptr eptr) {
                K2ERROR("Executor: exception: " << eptr);

                return seastar::make_ready_future<>();
            })
            .finally([&] {
                K2INFO("Deleting execution tasks...");

                cleanupQueues();
                // at this point we are done; unblock the caller

                return seastar::make_ready_future<>();
            });
        });
    }

    virtual seastar::future<> stop()
    {
        if(_stopFlag) {
            return seastar::make_ready_future<>();
        }

         K2INFO("Stopping message service...");

          // set the flag to stop all services
        _stopFlag = true;

        _queue.releasePromises();

       return seastar::with_semaphore(_semaphore, 1, [&] {

            return seastar::make_ready_future<>();
        });
    }

private:
    seastar::future<ExecutorTaskPtr> getNextTask() {
        return _queue.popWithFuture();
    }

    void createPayload(ExecutorTaskPtr pTask)
    {
        // this satisfies the case where the client provides the payload as part of the execution
        if(pTask->_pClientData->_pPayload.get()) {
            pTask->_pPlatformData->_pPayload = std::move(pTask->_pClientData->_pPayload);
        }
        else {
            assert(pTask->_pPlatformData->_pEndpoint.get() != nullptr);
            const auto startTime = Clock::now();
            pTask->_pPlatformData->_pPayload = pTask->_pPlatformData->_pEndpoint->newPayload();
            const auto timePoint = Clock::now();
            _createPayloadLatency.add(timePoint - startTime);
        }
    }

    void createEndpoint(ExecutorTaskPtr pTask)
    {
        if(pTask->_pPlatformData->_pPayload.get()) {
            // endpoint is present
            return;
        }

        const auto startTime = Clock::now();
        auto& disp = _dispatcher.local();
        pTask->_pPlatformData->_pEndpoint = disp.getTXEndpoint(pTask->getUrl());
        _createEndpointLatency.add(Clock::now() - startTime);

        if (!pTask->_pPlatformData->_pEndpoint) {

            throw std::runtime_error("unable to create endpoint for url");
        }
    }

    seastar::future<> executeTask(ExecutorTaskPtr pTask)
    {
        createEndpoint(pTask);
        createPayload(pTask);

        // invoke the callback with payload
        if(pTask->hasPayloadCallback()) {
            const auto timePoint = Clock::now();
            pTask->invokePayloadCallback();
            _payloadCallbackTime.add(Clock::now() - timePoint);

            // in this case we just want to create the payload; we do not want to send the message
            if(pTask->_pClientData->_fPayloadPtr) {
                _queue.completeTask(pTask);

                return seastar::make_ready_future<>();
            }
        }

        // check the endpoint is present
        assert(pTask->_pPlatformData->_pEndpoint.get() != nullptr);
        // check the payload is present
        assert(pTask->_pPlatformData->_pPayload.get() != nullptr);
        return sendMessage(pTask)
            .then([&, pTask](std::unique_ptr<ResponseMessage> response) mutable {
                 pTask->_pPlatformData->_pResponse = std::move(response);

                return seastar::make_ready_future<>();
            })
            .handle_exception([&, pTask](std::exception_ptr eptr) {
                pTask->_pPlatformData->_pResponse.reset();
                try {
                    std::rethrow_exception(eptr);
                }
                catch(ExecutionException& e) {
                    pTask->_pPlatformData->_pResponse->status = e.getStatus();
                }
                catch (RPCDispatcher::RequestTimeoutException& e) {
                    pTask->_pPlatformData->_pResponse->status =  Status::TimedOut;
                }
                catch(...) {
                   pTask->_pPlatformData->_pResponse->status =  Status::UnknownError;
                }

                return seastar::make_ready_future<>();
            })
           .finally([&, pTask] {
                invokeCallback(pTask);

                return seastar::make_ready_future<>();
            });
    }

    seastar::future<std::unique_ptr<ResponseMessage>> sendMessage(ExecutorTaskPtr pTask)
    {
        const auto startTime = Clock::now();
        return _dispatcher.local().sendRequest(K2Verbs::PartitionMessages,
            std::move(pTask->_pPlatformData->_pPayload),
            *pTask->_pPlatformData->_pEndpoint,
            pTask->getTimeout())
        .then([this, startTime, ep=std::move(pTask->_pPlatformData->_pEndpoint)](std::unique_ptr<k2::Payload> payload) mutable{
            _sendMessageLatency.add(Clock::now() - startTime);
            // parse a ResponseMessage from the received payload
            auto readBytes = payload->getSize();
            auto hdrSize = sizeof(ResponseMessage::Header);
            ResponseMessage::Header header;
            payload->seek(0);
            payload->read(&header, hdrSize);

            if(Status::Ok != header.status) {
                throw ExecutionException(header.status, "Error status");
            }

            if(!header.messageSize) {
                return seastar::make_ready_future<std::unique_ptr<ResponseMessage>>(std::make_unique<ResponseMessage>(Endpoint(ep->getURL()), std::move(*ep->newPayload().release()), header));
            }

            auto&& buffers = payload->release();
            buffers[0].trim_front(hdrSize);

            if(header.messageSize != readBytes - hdrSize) {
                throw ExecutionException(Status::MessageParsingError, "Invalid message size");
            }
            return seastar::make_ready_future<std::unique_ptr<ResponseMessage>>(std::make_unique<ResponseMessage>(Endpoint(ep->getURL()), Payload(std::move(buffers), readBytes - hdrSize), header));
         });
    }

    bool hasTimerExpired()
    {
        return Clock::now() > _runTaskTimepoint;
    }

    void invokeCallback(ExecutorTaskPtr pTask)
    {
        const auto timePoint = Clock::now();
        pTask->invokeResponseCallback();
        _resultCallbackTime.add(Clock::now() - timePoint);
        _taskTime.add(Clock::now() - pTask->_pClientData->_startTime);
        _queue.completeTask(pTask);
    }

    void cleanupQueues()
    {
        // all the tasks wil be collected in the completed tasks queue and the platform data will be released
        _queue.collectClientData(_queue._readyTasks);
        _queue.collectClientData(_queue._completedTasks);
        deletePlatformData(_queue._readyTasks);
        deletePlatformData(_queue._completedTasks);
    }

    void deletePlatformData(boost::lockfree::spsc_queue<ExecutorTaskPtr>& tasks)
    {
        ExecutorTaskPtr pTask;
        while(tasks.pop(pTask)) {
            if(pTask.get() && pTask->_pPlatformData.get()) {
                pTask->_pPlatformData.release();
            }
           _queue._completedTasks.push(pTask);
        }
    }

    void registerMetrics()
    {
        std::vector<metrics::label_instance> labels;
        _metricGroups.add_group("transport", {
            metrics::make_histogram("task_lifecycle_time", [this] { return _taskTime.getHistogram(); }, metrics::description("Lifecycle time of the task"), labels),
            metrics::make_histogram("task_dequeue_time", [this] { return _dequeueTime.getHistogram(); }, metrics::description("Time the task spend waiting in the queue until it is dequeued"), labels),
            metrics::make_histogram("send_message_latency", [this] { return _sendMessageLatency.getHistogram(); }, metrics::description("Latency to send a single message"), labels),
            metrics::make_histogram("create_payload_latency", [this] { return _createPayloadLatency.getHistogram(); }, metrics::description("Time spend in the payload callback"), labels),
            metrics::make_histogram("create_endpoint_latency", [this] { return _createEndpointLatency.getHistogram(); }, metrics::description("Time it takes to create and transport endpoint"), labels),
            metrics::make_histogram("client_loop_time", [this] { return _clientLoopTime.getHistogram(); }, metrics::description("Time spend executing client loop"), labels),
            metrics::make_histogram("payload_callback_time", [this] { return _payloadCallbackTime.getHistogram(); }, metrics::description("Time spend executing the payload callback"), labels),
            metrics::make_histogram("result_callback_time", [this] { return _resultCallbackTime.getHistogram(); }, metrics::description("Time spend executing result callback"), labels),
            metrics::make_histogram("task_loop_idle_time", [this] { return _taskLoopIdleTime.getHistogram(); }, metrics::description("Time waiting until the next task loop invocation"), labels),
            metrics::make_histogram("client_loop_idle_time", [this] { return _clientLoopIdleTime.getHistogram(); }, metrics::description("Time waiting until the next client loop invocation"), labels),
        });
    }

}; // class MessageService

}; // namespace k2
