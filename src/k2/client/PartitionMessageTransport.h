#pragma once

#include <k2/common/PartitionMessage.h>
#include "BoostTransport.h"

namespace k2
{

template<typename MessageT>
std::unique_ptr<ResponseMessage> sendPartitionMessage(
                                                    const char* ip,
                                                    uint16_t port,
                                                    MessageType messageType,
                                                    PartitionAssignmentId partition,
                                                    const MessageT& messageContent
                                                    )
{
    MessageBuilder messageBuilder = MessageBuilder::request(KnownVerbs::PartitionMessages);

    PayloadWriter writer = messageBuilder.getWriter();
    PartitionMessage::serializeMessage(writer, messageType, partition, messageContent);

    BoostTransport transport(ip, port);
    std::unique_ptr<MessageDescription> message = transport.messageExchange(messageBuilder.build());

    ResponseMessage::Header header;
    if(!message->payload.getReader().read(header))
        throw std::exception();

    TIF(header.status);
    auto response = std::make_unique<ResponseMessage>(header);

    if(!header.messageSize)
        return response;

    size_t readBytes = message->payload.getSize();
    size_t hdrSize = sizeof(ResponseMessage::Header);

    auto&& buffers = message->payload.release();
    buffers[0].trim_front(hdrSize);
    assert(header.messageSize == readBytes - hdrSize);
    response->payload = Payload(std::move(buffers), readBytes - hdrSize);

    return response;
}

}  //  namespace k2
