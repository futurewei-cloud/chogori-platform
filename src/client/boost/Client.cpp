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
std::unique_ptr<Payload> __recvMessage(boost::asio::ip::tcp::socket& socket) {
    k2::RPCParser parser([]{return false;});
    std::unique_ptr<Payload> result;
    bool completed = false;
    parser.registerMessageObserver([&result, &completed](k2::Verb, k2::MessageMetadata, std::unique_ptr<Payload> payload) {
        result = std::move(payload);
        completed = true;
    });
    parser.registerParserFailureObserver([&completed](std::exception_ptr) {
        K2ERROR("Failed to receive message");
        completed = true;
    });

    boost::array<uint8_t, 1024> boostBuf;
    boost::system::error_code error;

    while (!completed) {
        size_t readBytes = socket.read_some(boost::asio::buffer(boostBuf), error);
        K2DEBUG("Read from socket: " << readBytes << " bytes" << ", error=" << error);
        if (error)
            throw boost::system::system_error(error); // Some error occurred

        assert(readBytes > 0);

        k2::Binary buf(boostBuf.data(), readBytes);
        parser.feed(std::move(buf));
        parser.dispatchSome();
    }
    K2DEBUG("Received message of size: "<< (result?result->getSize():0));
    return result;
}

std::unique_ptr<Payload> __createTransportPayload(Payload&& userData) {
    K2DEBUG("creating transport payload from data size=" << (userdata?userData->getSize():0));
    k2::MessageMetadata metadata;
    metadata.setRequestID(uint64_t(std::rand()));
    return k2::RPCParser::serializeMessage(
        std::move(userData), (k2::Verb)Constants::NodePoolMsgVerbs::PARTITION_MESSAGE, std::move(metadata));
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
