#include "Client.h"
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <iostream>

#include "boost/program_options.hpp"
#include "boost/filesystem.hpp"

#include <common/PartitionMetadata.h>

namespace k2
{

std::unique_ptr<ResponseMessage> sendMessage(const char* ipAndPort, const Payload& message)
{
    (void) ipAndPort; // TODO use me

    boost::asio::io_service ios;
    //boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string(host), port);
    boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string("127.0.0.1"), 11311);
    boost::asio::ip::tcp::socket socket(ios);
    socket.connect(endpoint);

    boost::asio::socket_base::message_flags flags;
    boost::system::error_code error;
    socket.send(message.toBoostBuffers(), flags, error);
    if(error)
    {
        std::cerr << "Error while sending message:" << error << std::endl << std::flush;
        throw std::exception();
    }

    boost::asio::streambuf receiveBuffer;
    size_t readBytes = boost::asio::read(socket, receiveBuffer.prepare(sizeof(ResponseMessage::Header)), boost::asio::transfer_exactly(sizeof(ResponseMessage::Header)), error);
    assert(readBytes == sizeof(ResponseMessage::Header));
    receiveBuffer.commit(readBytes);
    ResponseMessage::Header* header = (ResponseMessage::Header*)boost::asio::detail::buffer_cast_helper(receiveBuffer.data());
    TIF(header->status);

    auto response = std::make_unique<ResponseMessage>(*header);
    size_t messageSize = header->messageSize;
    if(!messageSize)
        return response;
    receiveBuffer.consume(readBytes);

    readBytes = boost::asio::read(socket, receiveBuffer.prepare(messageSize), boost::asio::transfer_exactly(messageSize), error);
    assert(readBytes == messageSize);
    receiveBuffer.commit(readBytes);
    Binary data(readBytes);
    memcpy(data.get_write(), boost::asio::detail::buffer_cast_helper(receiveBuffer.data()), readBytes);

    std::vector<Binary> buffers;
    buffers.push_back(std::move(data));
    response->payload = Payload(std::move(buffers), readBytes);

    socket.close();

    return response;
}

}  //  namespace k2