// k2
#include "Client.h"
#include <client/executor/Executor.h>

namespace k2
{

namespace client
{

Client::Client()
: _executor(*this)
{
    // empty
}

Client::~Client()
{
    _executor.stop();
}

void Client::init(const ClientSettings& settings)
{
    ASSERT(!settings.networkProtocol.empty())
    _settings = settings;
    _executor.init(settings);
    _executor.start();
}

Payload Client::createPayload()
{
    std::string endpoint = _settings.networkProtocol + "://0.0.0.0:0000";
    return std::move(*createPayload(endpoint).release());
}

void Client::createPayload(std::function<void(Payload&&)> onCompleted) {
    std::string endpoint = _settings.networkProtocol + "://0.0.0.0:0000";

    _executor.execute(endpoint, [onCompleted] (std::unique_ptr<k2::Payload> pPayload) mutable {
        PartitionMessage::Header* header;
        bool ret = pPayload->getWriter().reserveContiguousStructure(header);
        ASSERT(ret);
        onCompleted(std::move(*pPayload.release()));
    });
}

void Client::execute(Operation&& operation, std::function<void(IClient&, OperationResult&&)> onCompleted)
{
    // we expect at least one message
    ASSERT(!operation.messages.empty());
    // find all the endpoints for the range
    // TODO: iterate through all the messages
    Range& range = *(operation.messages.begin()->ranges.begin());
    std::unique_ptr<Payload> pSourcePayload = std::make_unique<Payload>(std::move(operation.messages.begin()->content));

    auto it = _partitionMap.find(range);
    ASSERT(it != _partitionMap.end());

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
    ASSERT(sourcePayload.getSize() >= headerSize);
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
    ASSERT(ret);

    return std::move(pPayload);
}

}; // mamespace client;

}; // namespace k2
