#pragma once

// std
#include <atomic>
// boost
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
// seastar
#include <seastar/core/reactor.hh>
// k2:transport
#include "transport/RPCDispatcher.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/RetryStrategy.h"
// k2:client
#include <client/IClient.h>
#include "ExecutorTask.h"
#include "ExecutorQueue.h"
#include "IService.h"

namespace k2
{

namespace metrics = seastar::metrics;

class MessageService: public seastar::weakly_referencable<MessageService>
{
public:
    struct Settings
    {
    public:
        bool _userInitThread = false;
        std::function<uint64_t(client::IClient&)> _clientLoopFn;
        client::IClient& _rClient;

        Settings(bool userInitThread,  std::function<uint64_t(client::IClient&)> clientLoopFn, client::IClient& rClient)
        : _userInitThread(userInitThread)
        , _clientLoopFn(clientLoopFn)
        , _rClient(rClient)
        {
            // empty
        }

        Settings(std::function<uint64_t(client::IClient&)> clientLoopFn, client::IClient& rClient)
        : _userInitThread(true)
        , _clientLoopFn(clientLoopFn)
        , _rClient(rClient)
        {
            // empty
        }

        Settings(client::IClient& rClient)
        : _userInitThread(false)
        , _clientLoopFn(nullptr)
        , _rClient(rClient)
        {
            // empty
        }

    };

    // Service launcher
    class Launcher: public IService
    {
    private:
        std::unique_ptr<seastar::distributed<MessageService>> _pDistributed;
        MessageService::Settings _settings;
        std::vector<std::unique_ptr<ExecutorQueue>>& _queues;

    public:
        Launcher(const MessageService::Settings& settings, std::vector<std::unique_ptr<ExecutorQueue>>& queues)
        : _settings(settings)
        , _queues(queues)
        {
            // EMPTY
        }

        virtual seastar::future<> init(k2::RPCDispatcher::Dist_t& rDispatcher) {
            if(nullptr == _pDistributed.get()) {
                _pDistributed = std::make_unique<seastar::distributed<MessageService>>();
            }

            return _pDistributed->start(_settings, std::ref(rDispatcher), std::ref(_queues));
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
    const std::chrono::microseconds _minDelay; // the minimum amount of time before calling the next application loop
    std::chrono::time_point<std::chrono::steady_clock> _runTaskTimepoint;
    // metrics
    std::chrono::time_point<std::chrono::steady_clock> _taskLoopScheduleTime;
    std::chrono::time_point<std::chrono::steady_clock> _clientLoopScheduleTime;
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
    Settings _settings;
    RPCDispatcher::Dist_t& _dispatcher;
    ExecutorQueue& _queue;

public:
    MessageService(
        Settings settings,
        RPCDispatcher::Dist_t& dispatcher,
        std::vector<std::unique_ptr<ExecutorQueue>>& _queues)
    : _minDelay(std::chrono::microseconds(5))
    , _settings(settings)
    , _dispatcher(dispatcher)
    , _queue(*(_queues[seastar::engine().cpu_id()].get()))
    {
        auto now = std::chrono::steady_clock::now();
        _runTaskTimepoint = now;
        _taskLoopScheduleTime = now;
        _clientLoopScheduleTime = now;
    }

    seastar::future<> start()
    {
        registerMetrics();

        std::vector<seastar::future<>> futures;

        // start the application loop
        if(_settings._userInitThread) {
            auto future = seastar::do_until([&] { return _stopFlag; }, [&] {
                auto timePoint = std::chrono::steady_clock::now();
                _clientLoopIdleTime.add(timePoint - _clientLoopScheduleTime);
                _clientLoopScheduleTime = timePoint;
                // execute client loop
                const long int timeslice = _settings._clientLoopFn(_settings._rClient);
                _clientLoopTime.add(std::chrono::steady_clock::now() - timePoint);
                const auto delay = (timeslice < _minDelay.count()) ? _minDelay : std::chrono::microseconds(timeslice);

                return seastar::sleep(std::move(delay));
            }).or_terminate();

            futures.push_back(std::move(future));
        }

        // start message polling
        auto future = seastar::do_until([&] { return _stopFlag && _queue.empty(); }, [&] {
            const auto now = std::chrono::steady_clock::now();
            _taskLoopIdleTime.add(now - _taskLoopScheduleTime);
            _taskLoopScheduleTime = now;

            return getNextTask().then([&] (ExecutorTaskPtr pTask) {
                if(!pTask || !pTask.get()) {

                    return seastar::sleep(std::chrono::microseconds(10));
                }

                const auto timePoint = std::chrono::steady_clock::now();
                _dequeueTime.add(timePoint - pTask->_pClientData->_startTime);
                pTask->_pPlatformData.reset(new ExecutorTask::PlatformData());

                // execute the task and invoke the client's callback
                return executeTask(pTask);
            });

        }).or_terminate();

        futures.push_back(std::move(future));

        // wait for all the futures to complete
        return seastar::when_all_succeed(futures.begin(), futures.end())
        .handle_exception([] (std::exception_ptr eptr) {
            K2ERROR("Executor: exception: " << eptr);

            return seastar::make_ready_future<>();
        })
        .finally([&] {
            K2INFO("Deleting execution tasks...");

            cleanupQueues();
            // at this point we are done; unblock the caller

            return seastar::make_ready_future<>();
        }).or_terminate();
    }

    seastar::future<> stop()
    {
        if(_stopFlag) {
            return seastar::make_ready_future<>();
        }

         K2INFO("Stopping message service...");
         // set the flag to stop all services
        _stopFlag = true;

        // poll until the service has gracefully stopped
        return seastar::do_until([&] { return _queue.empty() ; }, [&] {

            return seastar::sleep(std::move(std::chrono::seconds(1)));
        }).or_terminate();
    }

private:
    seastar::future<ExecutorTaskPtr> getNextTask()
    {
        // if it's running in the user thread; then the queue will block until a task is available
        if(_settings._userInitThread) {

            return _queue.popWithFuture();
        }
        // if it's running on a separate thread; the queue will poll; a semaphore can be used instead
        else {
            ExecutorTaskPtr pTask = _queue.pop();

            return seastar::make_ready_future<ExecutorTaskPtr>(pTask);
        }
    }

    void createPayload(ExecutorTaskPtr pTask)
    {
        if(!pTask->hasEndpoint()) {
            createEndpoint(pTask);
        }

        const auto startTime = std::chrono::steady_clock::now();
        pTask->_pPlatformData->_pPayload = pTask->_pPlatformData->_pEndpoint->newPayload();
        const auto timePoint = std::chrono::steady_clock::now();
        _createPayloadLatency.add(timePoint - startTime);
        // invoke payload callback
        pTask->invokePayloadCallback();
        _payloadCallbackTime.add(std::chrono::steady_clock::now() - timePoint);
    }

    void createEndpoint(ExecutorTaskPtr pTask)
    {
        const auto startTime = std::chrono::steady_clock::now();
        auto& disp = _dispatcher.local();
        pTask->_pPlatformData->_pEndpoint = disp.getTXEndpoint(pTask->getUrl());
        _createEndpointLatency.add(std::chrono::steady_clock::now() - startTime);

        if (!pTask->_pPlatformData->_pEndpoint) {
            throw std::runtime_error("unable to create endpoint for url");
        }
    }

    seastar::future<> executeTask(ExecutorTaskPtr pTask)
    {
        if(pTask->shouldCreatePayload()) {
            createPayload(pTask);
        }
        // this satisfies the case where the client provides the payload as part of the execution
        else if(pTask->_pClientData->_pPayload.get()) {
            pTask->_pPlatformData->_pPayload = std::move(pTask->_pClientData->_pPayload);
        }

        // in this case we just want to invoke the payload callback
        if(!pTask->hasResponseCallback()) {
            _queue.completeTask(pTask);

            return seastar::make_ready_future<>();
        }

        if(!pTask->hasEndpoint()) {
            createEndpoint(pTask);
        }

        return sendMessage(pTask)
            .then([&, pTask](std::unique_ptr<ResponseMessage> response) mutable {
                 pTask->_pPlatformData->_pResponse = std::move(response);

                return seastar::make_ready_future<>();
            })
            .handle_exception([&, pTask](std::exception_ptr eptr) {
                pTask->_pPlatformData->_pResponse.reset(new ResponseMessage());
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
        const auto startTime = std::chrono::steady_clock::now();

        return _dispatcher.local().sendRequest(KnownVerbs::PartitionMessages,
            std::move(pTask->_pPlatformData->_pPayload),
            *pTask->_pPlatformData->_pEndpoint,
            pTask->getTimeout())
        .then([this, startTime = std::move(startTime)](std::unique_ptr<k2::Payload> payload) {
            _sendMessageLatency.add(std::chrono::steady_clock::now() - startTime);
            // parse a ResponseMessage from the received payload
            auto readBytes = payload->getSize();
            auto hdrSize = sizeof(ResponseMessage::Header);
            ResponseMessage::Header header;
            payload->getReader().read(&header, hdrSize);

            if(Status::Ok != header.status) {
                throw ExecutionException(header.status, "Error status");
            }

            auto response = std::make_unique<ResponseMessage>(header);

            if(!header.messageSize) {
                return seastar::make_ready_future<std::unique_ptr<ResponseMessage>>(std::move(response));
            }

            auto&& buffers = payload->release();
            buffers[0].trim_front(hdrSize);

            if(header.messageSize != readBytes - hdrSize) {
                throw ExecutionException(Status::MessageParsingError, "Invalid message size");
            }

            response->payload = Payload(std::move(buffers), readBytes - hdrSize);

            return seastar::make_ready_future<std::unique_ptr<ResponseMessage>>(std::move(response));
         });
    }

    bool hasTimerExpired()
    {
        return std::chrono::steady_clock::now() > _runTaskTimepoint;
    }

    void  invokeCallback(ExecutorTaskPtr pTask)
    {
        const auto timePoint = std::chrono::steady_clock::now();
        pTask->invokeResponseCallback();
        _resultCallbackTime.add(std::chrono::steady_clock::now() - timePoint);
        _taskTime.add(std::chrono::steady_clock::now() - pTask->_pClientData->_startTime);
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
                pTask->_pPlatformData->_pPayload->release();
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
