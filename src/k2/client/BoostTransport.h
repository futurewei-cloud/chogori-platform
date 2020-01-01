#pragma once

#include <k2/common/Log.h>
#include <k2/k2types/MessageVerbs.h>
#include <k2/transport/RPCParser.h>
#include <k2/transport/Status.h>
#include <boost/array.hpp>
#include <boost/asio.hpp>

namespace k2
{

static MessageMetadata newRequest() {
    MessageMetadata metadata;
    metadata.setRequestID(uint64_t(std::rand()));

    return metadata;
}

static MessageMetadata createResponse(const MessageMetadata& originalRequest) {
    MessageMetadata metadata = newRequest();
    metadata.setResponseID(originalRequest.requestID);
    return metadata;
}
//
//  Describes received messsage
//
struct MessageDescription {
    Verb verb;
    MessageMetadata metadata;
    Payload payload;

    MessageDescription() : verb(InternalVerbs::NIL) {}

    MessageDescription(Verb verb, MessageMetadata metadata, Payload payload) :
        verb(verb), metadata(std::move(metadata)), payload(std::move(payload)) {}
};

//
//  TODO: below given simple interface to do message reading and writting. Implementation are not efficient, since parser
//  currently is not based on streaming (PayloadWriter/PayloadReader) interfaces. We need to change parser to make it more
//  simple, so we can integrate it with MessageReader/Builder in more efficient way.
//

//
//  Simple message reader, which works as sink-source pattern for buffers-messages.
//  Customer of MessageReaders provides incoming data buffers using putBuffer function and collect
//  parsed messages using getMessage function.
//
class MessageReader {
   protected:
    std::queue<Binary> inBuffers;
    std::queue<MessageDescription> outMessages;
    RPCParser parser;
    Status status = Status::Ok;

   public:
    MessageReader() : parser([] { return false; }) {
        parser.registerMessageObserver([this](Verb verb, MessageMetadata metadata, std::unique_ptr<Payload> payload) {
            MessageDescription msg;
            msg.verb = verb;
            msg.metadata = metadata;
            if (payload) {
                msg.payload = std::move(*payload);
            }
            outMessages.push(std::move(msg));
        });

        parser.registerParserFailureObserver([this](std::exception_ptr) {
            status = Status::MessageParsingError;
        });
    }

    Status getStatus() { return status; }

    bool putBuffer(Binary binary) {
        if (status != Status::Ok)
            return false;

        inBuffers.push(std::move(binary));
        return true;
    }

    bool getMessage(MessageDescription& outMessage) {
        while (true) {
            if (!outMessages.empty()) {
                outMessage = std::move(outMessages.front());
                outMessages.pop();
                return true;
            }

            if (status != Status::Ok || inBuffers.empty()) {
                return false;
            }
            parser.feed(std::move(inBuffers.front()));
            inBuffers.pop();
            parser.dispatchSome();
        }

        return false;
    }

    Status readMessage(MessageDescription& outMessage, std::function<Status(Binary& outBuffer)> bufferSource) {
        while (true) {
            Binary buffer;
            RET_IF_BAD(bufferSource(buffer));

            putBuffer(std::move(buffer));
            if (getMessage(outMessage))
                return Status::Ok;

            RET_IF_BAD(status);
        }
    }

    static Status readSingleMessage(MessageDescription& outMessage, std::function<Status(Binary& outBuffer)> bufferSource) {
        MessageReader reader;
        return reader.readMessage(outMessage, std::move(bufferSource));
    }
};

//
//  Create messages to send
//
class MessageBuilder {
public:
    MessageDescription message;
protected:
    Payload::PayloadPosition headerPosition;
    size_t headerSize;

    void writeHeader() {
        Binary header(txconstants::MAX_HEADER_SIZE);
        headerSize = RPCParser::serializeHeader(header, message.verb, message.metadata);

        message.payload.seek(headerPosition);
        message.payload.write(header.get(), header.size());
    }

    MessageBuilder(Verb verb, MessageMetadata metadata, Payload payload) :
        message(verb, metadata, std::move(payload)),
        headerPosition(message.payload.getCurrentPosition()) {
        //  Write header just to estimate it's size and skip it
        message.metadata.setPayloadSize(1);  //  TODO: if we don't set size, RPCParser will not allocate enough space for it - we need to make this field static later
        writeHeader();
    }

public:
    static MessageBuilder request(Verb verb, MessageMetadata metadata, Payload payload) {
        return MessageBuilder(verb, std::move(metadata), std::move(payload));
    }

    static MessageBuilder request(Verb verb, Payload payload) {
        return MessageBuilder(verb, newRequest(), std::move(payload));
    }

    static MessageBuilder response(Payload newMessagePayload, const MessageMetadata& originalRequestMessageMetadata) {
        return MessageBuilder(InternalVerbs::NIL, createResponse(originalRequestMessageMetadata), std::move(newMessagePayload));
    }

    static MessageBuilder response(const MessageMetadata& originalRequestMessageMetadata) {
        return response(Payload(), originalRequestMessageMetadata);
    }

    static MessageBuilder response(Payload newMessagePayload, const MessageDescription& originalRequestMessage) {
        return response(std::move(newMessagePayload), originalRequestMessage.metadata);
    }

    template <typename... ArgsT>
    MessageBuilder& write(ArgsT&... args) {
        message.payload.writeMany(args...);
        return *this;
    }

    Payload&& build() {
        int64_t payloadSize = message.payload.getSize() - headerSize;
        assert(payloadSize > 0);
        message.metadata.payloadSize = payloadSize;
        writeHeader();  //  Write header one more time now with updated size
        return std::move(message.payload);
    }
};

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
        socket.send(shared_const_buffer(std::move(sendPayload)), flags, error);
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
    std::unique_ptr<MessageDescription> messageExchange(Verb verb, const RequestT& request, Payload&& payload)
    {
        return messageExchange(MessageBuilder::request(verb, std::move(payload)).write(request).build());
    }

    template<typename RequestT, typename ResponseT>
    void messageExchange(Verb verb, const RequestT& request, ResponseT& response, Payload&& payload)
    {
        std::unique_ptr<MessageDescription> messageDescription = messageExchange(verb, request, std::move(payload));
        messageDescription->payload.seek(0);
        if (!messageDescription->payload.read(response))
            throw std::exception();
    }

    template<typename RequestT, typename ResponseT>
    static void messageExchange(const char* ip, uint16_t port, Verb verb, const RequestT& request, ResponseT& response, Payload&& payload)
    {
        BoostTransport transport(ip, port);
        transport.messageExchange(verb, request, response, std::move(payload));
    }

    template<typename RequestT, typename ResponseT>
    static void messageExchange(const char* address, Verb verb, const RequestT& request, ResponseT& response, Payload&& payload)
    {
        constexpr size_t maxHostSize = 1024;

        K2ASSERT(strlen(address) < maxHostSize, "address size too big");
        char host[maxHostSize];

        int port;
        auto rcode = sscanf(address, "%[^:]:%d", host, &port) == 2;
        K2ASSERT(rcode, "unable to parse address");

        BoostTransport::messageExchange(host, port, verb, request, response, std::move(payload));
    }

    static std::unique_ptr<Payload> envelopeTransportPayload(Payload&& userData)
    {
        MessageMetadata metadata;
        metadata.setRequestID(uint64_t(std::rand()));
        metadata.setPayloadSize(userData.getSize());

        return RPCParser::serializeMessage(std::move(userData), K2Verbs::PartitionMessages, std::move(metadata));
    }
};  //  class BoostTransport

}   //  namespace k2
