#pragma once

// std
#include <atomic>
// boost
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
// seastar
#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
// k2:transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"
// k2:client
#include <client/IClient.h>
#include "ExecutorTask.h"
#include "ExecutorQueue.h"

namespace k2
{

class TransportPlatform
{
public:
    typedef seastar::distributed<TransportPlatform> Dist_t;

    struct Settings
    {
    public:
        bool _userInitThread;
        std::function<uint64_t(client::IClient&)> _clientLoopFn;
        client::IClient& _rClient;

        Settings(bool userInitThread,  std::function<uint64_t(client::IClient&)> clientLoopFn, client::IClient& rClient)
        : _userInitThread(userInitThread)
        , _clientLoopFn(clientLoopFn)
        , _rClient(rClient)
        {
            // empty
        }
    };

private:
    struct ExecutionException: public std::exception
    {
        std::exception_ptr _cause;
        Status _status;
        std::string _message = "Exception thrown during execution.";

        ExecutionException(Status status)
        : _status(status)
        {
            // empty
        }

        ExecutionException(Status status, std::string message)
        : _status(status), _message(std::move(message))
        {
            // empty
        }

        const std::string& what()
        {
            return _message;
        }
    };

    // class defined
    bool _stopFlag = false;
    std::chrono::time_point<std::chrono::steady_clock> _runTaskTimepoint;
    // from arguments
    Settings _settings;
    ExecutorQueue& _queue;
    RPCDispatcher::Dist_t& _dispatcher;

public:
    TransportPlatform(
        Settings settings,
        ExecutorQueue& queue,
        RPCDispatcher::Dist_t& dispatcher)
    : _settings(settings)
    , _queue(queue)
    , _dispatcher(dispatcher)
    {
         _runTaskTimepoint = std::chrono::steady_clock::now();
    }

    seastar::future<> start()
    {
        return seastar::do_until([&] { return _stopFlag && _queue.empty(); }, [&] {
            // invoke the client's loop only if initialized within the client's thread
            if(_settings._userInitThread && hasTimerExpired()) {
                // execute client loop
                uint64_t timeslice = _settings._clientLoopFn(_settings._rClient);
                // set next interval
                std::chrono::microseconds microSeconds(timeslice);
                _runTaskTimepoint = std::chrono::steady_clock::now() + microSeconds;
            }

            ExecutorTaskPtr pTask = _queue.pop();
            if (pTask) {
                pTask->_pPlatformData.reset(new ExecutorTask::PlatformData());

                // execute the task and invoke the client's callback
                return executeTask(pTask);
            }

            return seastar::make_ready_future<>();
        })
        .handle_exception([] (std::exception_ptr eptr) {
            K2ERROR("Executor: exception: " << eptr);

            return seastar::make_ready_future<>();
        })
        .finally([&] {
            K2INFO("Deleting execution tasks...");

            cleanupQueues();

            return seastar::make_ready_future<>();
        });
    }

    seastar::future<> stop()
    {
         K2INFO("Stopping transport...");
        _stopFlag = true;

        return seastar::make_ready_future<>();
    }

private:
    void createPayload(ExecutorTaskPtr pTask)
    {
        if(!pTask->hasEndpoint()) {
            createEndpoint(pTask);
        }
        pTask->_pPlatformData->_pPayload = pTask->_pPlatformData->_pEndpoint->newPayload();
        pTask->invokePayloadCallback();
    }

    void createEndpoint(ExecutorTaskPtr pTask)
    {
        auto& disp = _dispatcher.local();
        pTask->_pPlatformData->_pEndpoint = disp.getTXEndpoint(pTask->getUrl());
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
                    pTask->_pPlatformData->_pResponse->status = e._status;
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
        return _dispatcher.local().sendRequest(KnownVerbs::PartitionMessages,
            std::move(pTask->_pPlatformData->_pPayload),
            *pTask->_pPlatformData->_pEndpoint,
            pTask->getTimeout())
        .then([](std::unique_ptr<k2::Payload> payload) {
            // parse a ResponseMessage from the received payload
            auto readBytes = payload->getSize();
            auto hdrSize = sizeof(ResponseMessage::Header);
            K2DEBUG("transport read " << readBytes << " bytes. Reading header of size " << hdrSize);
            ResponseMessage::Header header;
            payload->getReader().read(&header, hdrSize);
            K2DEBUG("read header: status=" << getStatusText(header.status) << ", msgsize=" << header.messageSize << ", modulecode="<< header.moduleCode);

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
            K2DEBUG("Message received. returning response...");

            return seastar::make_ready_future<std::unique_ptr<ResponseMessage>>(std::move(response));
         });
    }

    bool hasTimerExpired()
    {
        return std::chrono::steady_clock::now() > _runTaskTimepoint;
    }

    void  invokeCallback(ExecutorTaskPtr pTask)
    {
        pTask->invokeResponseCallback();
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

}; // class TransportPlatform

}; // namespace k2
