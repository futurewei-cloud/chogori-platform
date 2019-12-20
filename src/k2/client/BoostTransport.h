#pragma once

#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <k2/transport/RPCParser.h>
#include <k2/transport/Status.h>
#include <k2/common/Log.h>


namespace k2
{

//
//  BoostTranport serializes messages into K2 format and send them through the wire using Boost.Asio tranport.
//  It's a slow transport latency-wise, so should be used only for non-performance critical tasks.
//  It also generate exception on errors.
//
class BoostTransport
{
protected:
    boost::asio::io_service ios;
    boost::asio::ip::tcp::endpoint endpoint;
    boost::asio::ip::tcp::socket socket;
public:
    BoostTransport(const char* ip, uint16_t port) : endpoint(boost::asio::ip::address::from_string(ip), port), socket(ios)
    {
        socket.connect(endpoint);
    }

    class shared_const_buffer {
        // adapter class for converting Payloads into boost const buffers
        public:
            explicit shared_const_buffer(Payload&& payload)
                : _byteCount(payload.getSize()),
                _data(payload.release()),
                _buffer(_data.size()) {
                for (size_t i = 0; i < _data.size() && _byteCount > 0; ++i) {
                    size_t toShare = std::min(_data[i].size(), _byteCount);
                    _buffer.emplace_back(_data[i].get(), toShare);
                    _byteCount -= toShare;
                }
            }

            // Implement the ConstBufferSequence requirements.
            typedef std::vector<boost::asio::const_buffer>::const_iterator const_iterator;
            const_iterator begin() const { return _buffer.begin(); }
            const_iterator end() const { return _buffer.end(); }

           private:
            size_t _byteCount;
            std::vector<Binary> _data;
            std::vector<boost::asio::const_buffer> _buffer;
    };

    void sendRawData(Payload&& sendPayload)
    {
        boost::asio::socket_base::message_flags flags{};
        boost::system::error_code error;
        socket.send(shared_const_buffer(std::forward<Payload>(sendPayload)), flags, error);
        if(error)
        {
            K2ERROR("Error while sending message:" << error);
            throw std::exception();
        }
    }

    std::unique_ptr<MessageDescription> receiveMessage()
    {
        auto message = std::make_unique<MessageDescription>();
        THROW_IF_BAD(MessageReader::readSingleMessage(*message, [&](Binary& buffer)
            {
                boost::array<char, 16*1024> boostBuf;
                boost::system::error_code error;

                size_t readBytes = socket.read_some(boost::asio::buffer(boostBuf), error);
                if (error)
                    throw boost::system::system_error(error); // Some error occurred
                K2ASSERT(readBytes > 0, "unable to read bytes from socket");
                buffer = Binary(boostBuf.data(), readBytes);

                return Status::Ok;
            }));

        return message;
    }

    std::unique_ptr<MessageDescription> messageExchange(Payload&& payload)
    {
        sendRawData(std::move(payload));
        return receiveMessage();
    }

    template<typename RequestT>
    std::unique_ptr<MessageDescription> messageExchange(Verb verb, const RequestT& request)
    {
        return messageExchange(MessageBuilder::request(verb).write(request).build());
    }

    template<typename RequestT, typename ResponseT>
    void messageExchange(Verb verb, const RequestT& request, ResponseT& response)
    {
        std::unique_ptr<MessageDescription> messageDescription = messageExchange(verb, request);

        PayloadReader reader = messageDescription->payload.getReader();
        if(!reader.read(response))
            throw std::exception();
    }

    template<typename RequestT, typename ResponseT>
    static void messageExchange(const char* ip, uint16_t port, Verb verb, const RequestT& request, ResponseT& response)
    {
        BoostTransport transport(ip, port);
        transport.messageExchange(verb, request, response);
    }

    template<typename RequestT, typename ResponseT>
    static void messageExchange(const char* address, Verb verb, const RequestT& request, ResponseT& response)
    {
        constexpr size_t maxHostSize = 1024;

        K2ASSERT(strlen(address) < maxHostSize, "address size too big");
        char host[maxHostSize];

        int port;
        auto rcode = sscanf(address, "%[^:]:%d", host, &port) == 2;
        K2ASSERT(rcode, "unable to parse address");

        BoostTransport::messageExchange(host, port, verb, request, response);
    }

    static std::unique_ptr<Payload> envelopeTransportPayload(Payload&& userData)
    {
        MessageMetadata metadata;
        metadata.setRequestID(uint64_t(std::rand()));
        metadata.setPayloadSize(userData.getSize());

        return RPCParser::serializeMessage(std::move(userData), KnownVerbs::PartitionMessages, std::move(metadata));
    }
};  //  class BoostTransport

}   //  namespace k2
