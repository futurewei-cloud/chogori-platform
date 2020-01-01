#pragma once

#include <k2/k2types/PartitionMessage.h>
#include "BoostTransport.h"

namespace k2
{

Payload newPayload() {
    return Payload([]() {
        return Binary(1000);
    });
}

template<typename MessageT>
std::unique_ptr<ResponseMessage> sendPartitionMessage(
                                                    const char* ip,
                                                    uint16_t port,
                                                    MessageType messageType,
                                                    PartitionAssignmentId partition,
                                                    const MessageT& messageContent
                                                    )
{
    MessageBuilder messageBuilder = MessageBuilder::request(K2Verbs::PartitionMessages, newPayload());

    PartitionMessage::serializeMessage(messageBuilder.message.payload, messageType, partition, messageContent);

    BoostTransport transport(ip, port);
    std::unique_ptr<MessageDescription> message = transport.messageExchange(messageBuilder.build());

    ResponseMessage::Header header;
    message->payload.seek(0);
    if(!message->payload.read(header))
        throw std::exception();

    THROW_IF_BAD(header.status);

    size_t readBytes = message->payload.getSize();
    size_t hdrSize = sizeof(ResponseMessage::Header);
    auto&& buffers = message->payload.release();
    assert(header.messageSize == readBytes - hdrSize);

    buffers[0].trim_front(hdrSize);

    auto response = std::make_unique<ResponseMessage>(
        std::string(ip)+ ":" + std::to_string(port),
        Payload(std::move(buffers), readBytes - hdrSize),
        header);

    return response;
}

}  //  namespace k2
