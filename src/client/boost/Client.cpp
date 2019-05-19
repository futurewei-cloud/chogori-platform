#include "Client.h"
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <iostream>
#include <cstdlib>

#include "boost/program_options.hpp"
#include "boost/filesystem.hpp"

#include <common/PartitionMetadata.h>
#include "transport/RPCHeader.h"
#include "transport/RPCParser.h"
#include "common/Status.h"
#include "common/Constants.h"

namespace k2
{
std::unique_ptr<Payload> __recvMessage(boost::asio::ip::tcp::socket& socket)
{
    MessageDescription message;
    TIF(MessageReader::readSingleMessage(message, [&](Binary& buffer) {
        boost::array<uint8_t, 16*1024> boostBuf;
        boost::system::error_code error;

        size_t readBytes = socket.read_some(boost::asio::buffer(boostBuf), error);
        if (error)
            throw boost::system::system_error(error); // Some error occurred
        ASSERT(readBytes > 0);
        buffer = Binary(boostBuf.data(), readBytes);

        return Status::Ok;
    }));

    return std::make_unique<Payload>(std::move(message.payload));
}

std::unique_ptr<Payload> __createTransportPayload(Payload&& userData) {
    K2DEBUG("creating transport payload from data size=" << userData.getSize());
    MessageMetadata metadata;
    metadata.setRequestID(uint64_t(std::rand()));
    metadata.setPayloadSize(userData.getSize());

    return RPCParser::serializeMessage(std::move(userData), KnownVerbs::PartitionMessages, std::move(metadata));
}

std::unique_ptr<ResponseMessage> sendMessage(const char* ip, uint16_t port, Payload&& message)
{
    auto sendPayload = __createTransportPayload(std::move(message));

    boost::asio::io_service ios;
    //boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string(host), port);
    boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string(ip), port);
    boost::asio::ip::tcp::socket socket(ios);
    socket.connect(endpoint);

    boost::asio::socket_base::message_flags flags{};
    boost::system::error_code error;

    socket.send(sendPayload->toBoostBuffers(), flags, error);
    if(error)
    {
        K2ERROR("Error while sending message:" << error);
        throw std::exception();
    }

    // Now process a response
    std::unique_ptr<Payload> transportData = __recvMessage(socket);

    // parse a ResponseMessage from the received transportData
    auto readBytes = transportData->getSize();
    auto hdrSize = sizeof(ResponseMessage::Header);
    K2DEBUG("transport read " << readBytes << " bytes. Reading header of size " << hdrSize);
    ResponseMessage::Header header;
    transportData->getReader().read(&header, hdrSize);
    K2DEBUG("read header: status=" << getStatusText(header.status) << ", msgsize=" << header.messageSize << ", modulecode="<< header.moduleCode);

    TIF(header.status);
    auto response = std::make_unique<ResponseMessage>(header);

    if(!header.messageSize) {
        return response;
    }
    auto&& buffers = transportData->release();
    buffers[0].trim_front(hdrSize);
    assert(header.messageSize == readBytes - hdrSize);
    response->payload = Payload(std::move(buffers), readBytes - hdrSize);

    socket.close();
    K2DEBUG("Message received. returning response...");
    return response;
}

}  //  namespace k2
