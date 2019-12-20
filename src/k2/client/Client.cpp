// k2
#include "Client.h"
#include <k2/executor/Executor.h>
#include <k2/common/Log.h>

namespace k2
{

namespace client
{

Client::Client()
{
    // empty
}

Client::~Client()
{
    _executor.stop();
}

void Client::init(const ClientSettings& settings)
{
    K2ASSERT(!settings.networkProtocol.empty(), "network protocol cannot be empty");
    _settings = settings;
    _executor.init(settings);
    _executor.start();
}

void Client::init(client::ClientSettings& settings, std::shared_ptr<config::Config> pConfig)
{
    K2ASSERT(!settings.networkProtocol.empty(), "network protocol cannot be empty");

    for(auto pNodeConfig : pConfig->getClusterNodes()) {
        for(auto pPartitionConfig : pNodeConfig->getPartitions()) {
            PartitionDescription desc;
            PartitionAssignmentId id;
            id.parse(pPartitionConfig->getId().c_str());
            desc.nodeEndpoint = std::move(pNodeConfig->getTransport()->getEndpoint());
            desc.id = id;
            PartitionRange partitionRange;
            partitionRange.lowKey = pPartitionConfig->getRange()._lowerBound;
            partitionRange.highKey = pPartitionConfig->getRange()._upperBound;
            desc.range = std::move(partitionRange);
            _partitionMap.map.insert(std::move(desc));
        }
    }

    _settings = settings;
    _executor.init(settings, pConfig->getClientConfig());
    _executor.start();
}

 void Client::stop()
 {
     _executor.stop();
 }

Payload Client::createPayload()
{
    std::string endpoint = _settings.networkProtocol + "://0.0.0.0:0000";
    return std::move(*createPayload(endpoint).release());
}

void Client::createPayload(std::function<void(IClient&, Payload&&)> onCompleted) {
    std::string endpoint = _settings.networkProtocol + "://0.0.0.0:0000";

    IClient* pClient = this;

    _executor.execute(endpoint, [pClient, onCompleted] (std::unique_ptr<k2::Payload> pPayload) mutable {
        PartitionMessage::Header* header;
        bool ret = pPayload->getWriter().reserveContiguousStructure(header);
        K2ASSERT(ret, "unable to reserve space in payload");
        onCompleted(*pClient, std::move(*pPayload.release()));
    });
}

void Client::execute(Operation&& operation, std::function<void(IClient&, OperationResult&&)> onCompleted)
{
    // we expect at least one message
    K2ASSERT(!operation.messages.empty(), "operation message cannot be empty");
    // find all the endpoints for the range
    // TODO: iterate through all the messages
    Range& range = *(operation.messages.begin()->ranges.begin());
    std::unique_ptr<Payload> pSourcePayload = std::make_unique<Payload>(std::move(operation.messages.begin()->content));

    auto it = _partitionMap.find(range);
    K2ASSERT(it != _partitionMap.end(), "range does not exist in partition map");

    // keep a reference to the first partiton since it will be send last; we are doing this to prevent from copying the original message
    PartitionDescription& firstPartition = *it;
    ++it;
    std::shared_ptr<ResultCollector> pCollector = std::make_shared<ResultCollector>(*this, std::move(onCompleted));
    pCollector->_counter++;

    // copy and send the rest of the messages
    for(;it!=_partitionMap.end(); ++it) {
         pCollector->_counter++;
         // copy payload and send to multiple partitions
        auto pCopyPayload = createPayload((*it).nodeEndpoint);
        copyPayload(*pSourcePayload.get(), *pCopyPayload.get());
        writePartitionHeader(*pCopyPayload.get(), *it);
        sendPayload((*it).nodeEndpoint, std::move(pCopyPayload), pCollector);
    }

    // send the original message last
    writePartitionHeader(*pSourcePayload.get(), firstPartition);
    sendPayload(firstPartition.nodeEndpoint, std::move(pSourcePayload), pCollector);
}

void Client::execute(PartitionDescription& partition, std::function<void(Payload&)> onPayload, std::function<void(IClient&, OperationResult&&)> onCompleted)
{
    _executor.execute(partition.nodeEndpoint, _defaultTimeout,
        [this, partition, onPayload] (Payload& payload) mutable {
            onPayload(payload);
            writePartitionHeader(payload, partition);
        },
        [this, onCompleted] (std::unique_ptr<ResponseMessage> message) mutable {
            OperationResult result;
            OperationResponse response;
            response.status = message->getStatus();
            response.moduleCode = message->getModuleCode();
            response.payload = std::move(message->payload);
            result._responses.push_back(std::move(response));
            // calback
            onCompleted(*this, std::move(result));
        });
}

void Client::runInThreadPool(std::function<void(IClient&)> routine)
{
    // TODO: implement method
    (void)routine;
    throw std::runtime_error("method not implemented");
}

// PRIVATE & PROTECTED

void Client::writePartitionHeader(Payload& payload, PartitionDescription& partition)
{
    PartitionMessage::Header header;
    header.messageType = MessageType::ClientRequest;
    header.partition = partition.id;
    header.messageSize = payload.getSize() - txconstants::MAX_HEADER_SIZE - sizeof(PartitionMessage::Header);
    auto writer = payload.getWriter(txconstants::MAX_HEADER_SIZE);
    writer.write(header);
}

void Client::sendPayload(const std::string& endpoint, std::unique_ptr<Payload> pPayload, std::shared_ptr<ResultCollector> pCollector)
{
    _executor.execute(endpoint, _defaultTimeout, std::move(pPayload), [this, pCollector] (std::unique_ptr<ResponseMessage> message) mutable {
        OperationResponse response;
        response.status = message->getStatus();
        response.moduleCode = message->getModuleCode();
        response.payload = std::move(message->payload);

        pCollector->collect(std::move(response));
    });
}

void Client::copyPayload(Payload& sourcePayload, Payload& destinationPayload)
{
    const size_t headerSize = txconstants::MAX_HEADER_SIZE + sizeof(PartitionMessage::Header);
    K2ASSERT(sourcePayload.getSize() >= headerSize, "");
    const size_t payloadSize = sourcePayload.getSize() - headerSize;
    auto reader = sourcePayload.getReader(headerSize);
    auto writer = destinationPayload.getWriter();
    writer.write(reader, payloadSize);
}

std::unique_ptr<Payload> Client::createPayload(const std::string& endpoint)
{
    std::mutex mutex;
    std::condition_variable conditional;
    std::unique_lock<std::mutex> lock(mutex);

    // create payload
    std::unique_ptr<Payload> pPayload;
    _executor.execute(endpoint, [&] (std::unique_ptr<k2::Payload> payload) mutable {
        pPayload = std::move(payload);
        conditional.notify_one();
    });

    // wait until the payload is created
    conditional.wait_for(lock, _defaultTimeout);

    if(!pPayload.get()) {
        throw std::runtime_error("Unable to create payload!");
    }

    // reserve space for the partition header
    PartitionMessage::Header* header;
    bool ret = pPayload->getWriter().reserveContiguousStructure(header);
    assert(ret);

    return std::move(pPayload);
}

std::vector<PartitionDescription> Client::getPartitions(Range& range)
{
    std::vector<PartitionDescription> partitions;
    auto it = _partitionMap.find(range);
    while(it != _partitionMap.end()) {
        partitions.push_back(*it);
        ++it;
    }

    return std::move(partitions);
}


}; // mamespace client;

}; // namespace k2
