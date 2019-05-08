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
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
// k2
#include <node/NodePool.h>
#include "IClient.h"

namespace k2
{

//
// Executor service for asynchronous communication with a K2 node.
//
// For every request, the executor will create a Task which contains a callback to the client and the Payload destined for a K2 node. These tasks are put into
// a queue which the executor periodically polls. The task is considered complete when the K2 request is performed and the client callback is invoked.
//
// The Executor operates in two modes:
// - User initialized thread: in this mode, the Seastar platform is executing in the client's thread.
// - Thread pool: in this mode, the Seastar platform is running in a separate thread and communication with the client's thread is done via shared queues.
//
// The thread pool mode can get a bit tricky since states between threads have to be shared and the memory management can be different from that of the Seatar platform.
// To make this easier, the following conventions have been established:
// - The states have been separated into Client thread responsibility and Seatar platform responsibility. Shared states are commented accordingly.
// - Memory allocated in the Client thread is dealocated by the client thread.
// - Memory allocated in the Seastar platform is deallocated by the Seastar platform.
// - Shared states are thread-safe.
//

class Executor
{
    // function which is invoked to propagate the response back to the client
    typedef std::function<void(ResponseMessage&)> ResponseCallback;
    // function which is invoked by the Seastar platform to create the Payload for the request
    typedef std::function<Payload(void)> PayloadFunctor;

private:
    struct ExecutionException: public std::exception {
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

    //
    // Data for which the Client thread is responsible for.
    //
    struct ClientData {
        ClientData()
        {
            // empty
        }

        ClientData(NodeEndpointConfig endpoint,
            ResponseCallback responseCallbackFn,
            PayloadFunctor payloadFn)
        : _endpoint(std::move(endpoint))
        , _callback(std::move(responseCallbackFn))
        , _createPayloadFn(std::move(payloadFn))
        {
            // empty
        }

        NodeEndpointConfig _endpoint;
        ResponseCallback _callback = nullptr;
        PayloadFunctor _createPayloadFn = nullptr;
    };

    //
    // Data for which the Seastar platform is responsible for.
    //
    struct SeastarData {
        std::unique_ptr<ResponseMessage> _pResponse = nullptr;
        Payload _payload;
    };

    //
    // Task representing a message that needs to be sent to a K2 endpoint.
    //
    // Memory management can get a bit complex here, so the responsibilities are divided as following:
    // - The Client thread is responsible for the lifecycle of the task object and all the members, except from the payload and the response.
    // - The Seastar platform is responsible for the lifecycle of the Payload and the response.
    //
    struct ExecutorTask
    {
        std::unique_ptr<ClientData> _pClientData = nullptr;
        // we do not want this object to be deleted when the destructor of the task is invoked
        SeastarData* _pSeastarData = nullptr;

        NodeEndpointConfig& getEndpoint() {
            return _pClientData->_endpoint;
        }

        //
        // Returns true when the create payload function is provided, false otherwise.
        //
        bool shouldCreatePayload() {
            return nullptr != _pClientData->_createPayloadFn;
        }

        auto& getPayload() {
            return _pSeastarData->_payload;
        }

        void createPayload() {
            _pSeastarData->_payload = std::move((_pClientData->_createPayloadFn)());
        }

        void invokeCallback() {
            (_pClientData->_callback)(*_pSeastarData->_pResponse.get());
        }
    };

    struct Connection
    {
        seastar::socket_address _address;
        seastar::connected_socket _socket;
        seastar::input_stream<char> _input;
        seastar::output_stream<char> _output;
    };

    // TODO: this is arbitrary defined
    static constexpr int _MAX_QUEUE_SIZE = 10;

    // shared members between the Client thread and the Seastar platform
    boost::lockfree::spsc_queue<ExecutorTask*> _readyTasks{_MAX_QUEUE_SIZE};
    boost::lockfree::spsc_queue<ExecutorTask*> _callbackTasks{_MAX_QUEUE_SIZE};
    boost::lockfree::spsc_queue<ExecutorTask*> _completedTasks{_MAX_QUEUE_SIZE};
    std::atomic<bool> _initFlag = false;
    std::atomic<bool> _stopFlag = false;
    std::atomic<bool> _userInitThread = false;
    // the mutex here is not used for synchronization, but to prevent the threads from spinning and wasting resources
    std::mutex _mutex;
    std::condition_variable _conditional;

    // client members
    std::thread _seastarThread;
    std::thread _callbackThread;

    // seastar members
    std::chrono::time_point<std::chrono::high_resolution_clock> _runTaskTimepoint;
    client::ClientSettings _settings;
    client::IClient* _pClient;

public:
    Executor()
    {
        _runTaskTimepoint = std::chrono::high_resolution_clock::now();
        _initFlag = false;
        _stopFlag = false;
        _userInitThread = false;
        _pClient = nullptr;
    }

    ~Executor()
    {
        stop();
        deleteTasks(_readyTasks);
        deleteTasks(_callbackTasks);
        deleteTasks(_completedTasks);
    }

    //
    // Initialize the executor.
    //
    // throws: runtime_error if the the executor is already initialized.
    //
    void init(const client::ClientSettings& settings, client::IClient* pClient)
    {
        if (_initFlag.load()) {
            throw std::runtime_error("Executor: attempted to re-initialize!");
        }

        _settings = settings;
        _pClient = pClient;
        _userInitThread = _settings.userInitThread;
        _stopFlag = false;
        _initFlag = true;

        // create all the task objects
        for(int i = 0; i < _MAX_QUEUE_SIZE; i++) {
            _completedTasks.push(new ExecutorTask());
        }
    }

    //
    // Start the executor.
    //
    // throws: runtime_error if the the executor is not initialized
    //
    void start()
    {
        if (!_initFlag.load()) {
            throw std::runtime_error("Executor: attempted to start without initializing!");
        }

        // if client initialized, run the Seastar platform in the client's thread
        if(_userInitThread.load()) {
            runSeastar();
        }
        else {
             // create separate threads to run the Seastar platform
            _seastarThread = std::thread([this] {
                runSeastar();
            });

            // We have to use a separate thread to invoke the client callback since any memory claimed during the callback
            // needs it be allocated in the client's context. Also we do not want the Seastar platform from being blocked on the callback.
            _callbackThread = std::thread([this] {
                while (!_stopFlag.load()) {
                    conditionallyWait();
                }
            });
        }
    }

    //
    // Stop the executor.
    //
    void stop()
    {
        _stopFlag = true;

        if(!_userInitThread.load()) {
            if(_seastarThread.joinable()) {
                _seastarThread.join();
            }

            if(_callbackThread.joinable()) {
                _callbackThread.join();
            }
        }
    }

    //
    // Schedules the request for execution. This API takes two functions as arguments. The first function is invoked in the Seastar platform to create the payload.
    // The second function is a callback to the client to return the result.
    //
    // throws: runtime_error if the executor is not initialized or the executor pipeline is full
    //
    void execute(NodeEndpointConfig endpoint, std::function<Payload(void)> createPayloadFn, std::function<void(ResponseMessage&)> callbackFn) {
        if (!_initFlag.load()) {
            throw std::runtime_error("Executor: attempted to start without initializing!");
        }

        ExecutorTask* pTask = nullptr;
        if(_completedTasks.pop(pTask)) {
            pTask->_pClientData.reset(new ClientData(std::move(endpoint), std::move(callbackFn), std::move(createPayloadFn)));
        }
        else {
            throw std::runtime_error("Executor: busy; retry");
        }

        // we messed up the task lifecycle at some point
        assert(_readyTasks.push(pTask));
        // wakeup all waiting threads
        _conditional.notify_all();
    }

private:

    // SHARED METHODS

    void invokeCallback() {
        ExecutorTask* pTask = nullptr;
        if(_callbackTasks.pop(pTask)) {
            pTask->invokeCallback();
            // clean-up the task for the next run; we cannot delete the Seastar data at this point
            deleteClientData(pTask);
            assert(_completedTasks.push(pTask));
        }
    }

    void deleteTasks(auto& tasks) {
        ExecutorTask* pTask = nullptr;
        while(tasks.pop(pTask)) {
            deleteSeastarData(pTask);
            delete pTask;
        }
    }

    void deleteClientData(auto* pTask) {
        if(pTask && pTask->_pClientData.get()) {
            pTask->_pClientData.reset();
        }
    }

    // CLIENT METHODS

    void conditionallyWait() {
        std::unique_lock<std::mutex> lock(_mutex);
        // TODO: 1 millisecond is arbitrarily chosen
       _conditional.wait_for(lock, std::chrono::milliseconds(1));
    }

    // SEASTAR METHODS

    void deleteSeastarData(auto& tasks) {
        ExecutorTask* pTask = nullptr;
        while(tasks.pop(pTask)) {
           deleteSeastarData(pTask);
        }
    }

    void deleteSeastarData(auto* pTask) {
        if(pTask && pTask->_pSeastarData) {
            pTask->_pSeastarData->_payload.clear();
            delete pTask->_pSeastarData;
            pTask->_pSeastarData = nullptr;
        }
    }

    void scheduleCallback(ExecutorTask* pTask) {
        assert(_callbackTasks.push(pTask));
        _conditional.notify_all();
    }

    bool hasTimerExpired()
    {
        return std::chrono::high_resolution_clock::now() > _runTaskTimepoint;
    }

    void runSeastar()
    {
       char *argv[] = {(char *)"k2SeastarExecutor", nullptr};
       int argc = sizeof(argv) / sizeof(*argv) - 1;

        seastar::app_template _app;
        _app.run(argc, argv, [&] {
            seastar::engine().at_exit([&] {
                deleteSeastarData(_completedTasks);
                deleteSeastarData(_readyTasks);
                deleteSeastarData(_callbackTasks);

                return seastar::make_ready_future<>();
            });

            return seastar::do_until([&] { return _stopFlag.load(); }, [&] {

                // invoke the client's loop only if initialized within the client's thread
                if(_userInitThread.load() && hasTimerExpired()) {
                    // execute client loop
                    uint64_t timeslice = _settings.runInLoop(*_pClient);
                    // set next interval
                    std::chrono::microseconds microSeconds(timeslice);
                    _runTaskTimepoint = std::chrono::high_resolution_clock::now() + microSeconds;
                }

                ExecutorTask* pTask = nullptr;
                if (_readyTasks.pop(pTask)) {
                    // clean-up the data from the previous run; this is the only safe place to perform the clean-up
                    deleteSeastarData(pTask);
                    pTask->_pSeastarData = new SeastarData();

                    if(pTask->shouldCreatePayload()) {
                        pTask->createPayload();
                    }
                    else {
                        // TODO: support API which takes the payload as an argument
                        throw std::runtime_error("Executor: no payload provided");
                    }

                    // execute the task and invoke the client's callback
                    return executeTask(pTask).then([&] {
                        invokeCallback();

                        return seastar::make_ready_future<>();
                    });
                }
                else if(_callbackTasks.empty()) {
                    // we do not have any more work to do since both the ready and callback queues are empty
                    conditionallyWait();
                }

                return seastar::make_ready_future<>();
            });
        });
    }

    seastar::future<> executeTask(ExecutorTask* pTask)
    {
        return connect(pTask->getEndpoint()).then([&, pTask] (auto pConnection) {
            return sendMessage(pConnection, std::move(pTask->getPayload()))
                .then([&, pTask](std::unique_ptr<ResponseMessage> response) mutable {
                    pTask->_pSeastarData->_pResponse = std::move(response);

                    return seastar::make_ready_future<>();
                });
        })
        .handle_exception([&, pTask](std::exception_ptr eptr) {
            pTask->_pSeastarData->_pResponse.reset(new ResponseMessage());
            try {
                std::rethrow_exception(eptr);
            }
            catch(ExecutionException& e) {
                 pTask->_pSeastarData->_pResponse->status = e._status;
            }
            catch(...) {
                pTask->_pSeastarData->_pResponse->status =  Status::UnknownError;
            }

            return seastar::make_ready_future<>();
        })
        .finally([&, pTask] {
            scheduleCallback(pTask);

            return seastar::make_ready_future<>();
        });
    }

    seastar::future<seastar::lw_shared_ptr<Connection>> connect(NodeEndpointConfig& endpoint)
    {
        seastar::socket_address address = seastar::make_ipv4_address(endpoint.ipv4.address, endpoint.ipv4.port);

        return seastar::engine().connect(address).then([this](auto socket) mutable {
            seastar::lw_shared_ptr<Connection> pConnection = seastar::make_lw_shared<Connection>();
            pConnection->_socket = std::move(socket);
            pConnection->_input = pConnection->_socket.input();
            pConnection->_output = pConnection->_socket.output();

            return seastar::make_ready_future<seastar::lw_shared_ptr<Connection>>(std::move(pConnection));
        });
    }

    seastar::future<> disconnect(seastar::lw_shared_ptr<Connection> pConnection)
    {
        return pConnection->_output.close().then([pConnection] {
            pConnection->_socket.shutdown_output();
            pConnection->_socket.shutdown_input();

            return seastar::make_ready_future<>();
        });
    }

    // TODO: replace with transport module
    seastar::future<std::unique_ptr<ResponseMessage>> sendMessage(seastar::lw_shared_ptr<Connection> pConnection, Payload message)
    {
        return write(pConnection, std::move(message)).then([this, pConnection] {
            return read(pConnection, sizeof(ResponseMessage::Header))
                .then([this, pConnection] (seastar::temporary_buffer<char>&& headerBuffer) {
                    if(headerBuffer.size() != sizeof(ResponseMessage::Header)) {
                         return seastar::make_exception_future<std::unique_ptr<ResponseMessage>>(ExecutionException(Status::MessageParsingError, "Unable to parse header"));
                    }

                    Binary headerBinary = moveBinary(headerBuffer);
                    ResponseMessage::Header* pHeader = (ResponseMessage::Header*)headerBinary.get_write();

                    try {
                        TIF(pHeader->status);
                    }
                    catch (...) {
                        return seastar::make_exception_future<std::unique_ptr<ResponseMessage>>(ExecutionException(pHeader->status, "Erroneous status"));
                    }

                    auto response = std::make_unique<ResponseMessage>(*pHeader);
                    size_t messageSize = pHeader->messageSize;
                    if(!messageSize) {
                        return seastar::make_ready_future<std::unique_ptr<ResponseMessage>>(std::move(response));
                    }

                    return read(pConnection, messageSize).then([&, pConnection = std::move(pConnection), messageSize] (seastar::temporary_buffer<char>&& payloadBuffer) {
                        size_t readBytes = payloadBuffer.size();

                        // throw exception
                        if(readBytes != messageSize) {
                            return seastar::make_exception_future<std::unique_ptr<ResponseMessage>>(ExecutionException(Status::MessageParsingError, "Unable to parse payload"));
                        }

                        Binary data(payloadBuffer.size());
                        memcpy(data.get_write(), payloadBuffer.get(), readBytes);

                        std::vector<Binary> buffers;
                        buffers.push_back(std::move(data));
                        response->payload = Payload(std::move(buffers), readBytes);

                        return seastar::make_ready_future<std::unique_ptr<ResponseMessage>>(std::move(response));
                    });
                });
        })
        .handle_exception([&, pConnection] (std::exception_ptr ep) {
            return disconnect(pConnection).then([&, ep] {
                return seastar::make_exception_future<std::unique_ptr<ResponseMessage>>(ep);
            });
        });
    }

    seastar::future<> write(seastar::lw_shared_ptr<Connection> pConnection, Payload message)
    {
        auto packet = Payload::toPacket(std::move(message));
        return pConnection->_output.write(std::move(packet)).then([pConnection = std::move(pConnection)] () {
                return pConnection->_output.flush();
         });
    }

    seastar::future<seastar::temporary_buffer<char>> read(seastar::lw_shared_ptr<Connection> pConnection, size_t size)
    {
        return pConnection->_input.read_exactly(size).then([] (seastar::temporary_buffer<char>&& headerBuffer) {
            return seastar::make_ready_future<seastar::temporary_buffer<char>>(std::move(headerBuffer));
        });
    }

}; // Executor

}; // namespace k2
