#pragma once

//std
#include <string>
#include <map>
#include <mutex>
// k2
#include <common/PartitionMetadata.h>
// k2:config
#include <config/Config.h>
// k2:client
#include <client/IClient.h>
#include <client/PartitionMap.h>
#include <client/executor/Executor.h>

namespace k2
{

namespace client
{

class Client: public IClient
{
private:
    struct ResultCollector
    {
        std::mutex _mutex;
        volatile int _counter = 0;
        OperationResult _result;
        IClient& _client;
        std::function<void(IClient&, OperationResult&&)> _callback;

        ResultCollector(IClient& client, std::function<void(IClient&, OperationResult&&)> callback)
        : _client(client)
        , _callback(callback)
        {
            // empty
        }

        void collect(OperationResponse&& response)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _result._responses.push_back(std::move(response));
            --_counter;

            if(_counter == 0) {
                _callback(_client, std::move(_result));
            }
        }
    };


protected:
    // TODO: make the timeout configurable
    const k2::Duration _defaultTimeout = std::chrono::milliseconds(10000);
    Executor _executor;
    PartitionMap _partitionMap;
    ClientSettings _settings;

public:
    Client();
    virtual ~Client();
    virtual void init(const ClientSettings& settings);
    virtual void stop();
    virtual Payload createPayload();
    virtual void createPayload(std::function<void(IClient&, Payload&&)> onCompleted);
    virtual void execute(Operation&& operation, std::function<void(IClient&, OperationResult&&)> onCompleted);
    virtual void execute(PartitionDescription& partition, std::function<void(Payload&)> onPayload, std::function<void(IClient&, OperationResult&&)> onCompleted);
    virtual void runInThreadPool(std::function<void(IClient&)> routine);


    //
    // Register an application to run; if an application is not registered the seastar platform will run in a separate thread.
    //
    template<typename T>
    void registerApplication(IApplication<T>& rApplication, T& rContext)
    {
        _executor.registerApplication<T>(rApplication, rContext);
    }

    //
    // Return the partitions for the given range.
    //
    std::vector<PartitionDescription> getPartitions(Range& range);

    //
    // Init the client from static configuration.
    //
    void init(client::ClientSettings& settings, std::shared_ptr<config::Config> pConfig);

protected:
    void sendPayload(const std::string& endpoint, std::unique_ptr<Payload> pPayload, std::shared_ptr<ResultCollector> pCollector);
    std::unique_ptr<Payload> createPayload(const std::string& endpoint);
    void copyPayload(Payload& sourcePayload, Payload& destinationPayload);
    void writePartitionHeader(Payload& payload, PartitionDescription& partition);

}; // class Client

}; // mamespace client;

}; // namespace k2
