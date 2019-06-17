#include "PoolMonitor.h"
#include "Node.h"
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <transport/RPCParser.h>

namespace k2
{
//
//  TODO: Unify with boost client
//
std::unique_ptr<Payload> recieveMessage(boost::asio::ip::tcp::socket& socket)
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

void sendData(boost::asio::ip::tcp::socket& socket, const Payload& sendPayload)
{
    boost::asio::socket_base::message_flags flags{};
    boost::system::error_code error;

    socket.send(sendPayload.toBoostBuffers(), flags, error);
    if(error)
    {
        K2ERROR("Error while sending message:" << error);
        throw std::exception();
    }
}

template<typename RequestT>
std::unique_ptr<Payload> messageExchange(const char* ip, uint16_t port, Verb verb, const RequestT& request)
{
    boost::asio::io_service ios;
    boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string(ip), port);
    boost::asio::ip::tcp::socket socket(ios);
    socket.connect(endpoint);

    sendData(socket, MessageBuilder::request(verb).write(request).build());

    return recieveMessage(socket);
}

template<typename RequestT>
std::unique_ptr<Payload> messageExchange(const char* address, Verb verb, const RequestT& request)
{
    constexpr size_t maxHostSize = 1024;

    ASSERT(strlen(address) < maxHostSize);
    char host[maxHostSize];

    int port;
    ASSERT(sscanf(address, "%s:%d", host, &port) == 2);

    return messageExchange(host, port, verb, request);
}

template<typename RequestT, typename ResponseT>
Status PoolMonitor::sendMessage(const RequestT& request, ResponseT& response)
{
    if(pool.getConfig().getPartitionManagerSet().empty())
        return LOG_ERROR(Status::NoPartitionManagerSetup);

    Status result;
    for(const auto& pm : pool.getConfig().getPartitionManagerSet())
    {
        try
        {
            std::unique_ptr<Payload> responsePayload = messageExchange(    //  TODO: Partition Manager can switch to different instance and let is know which is that
                                                            pm.c_str(),     //  also need to set time out
                                                            KnownVerbs::PartitionManager,
                                                            makeMessageWithType(request));

            PayloadReader reader = responsePayload->getReader();
            if(!reader.read(result))
            {
                result = Status::PartitionManagerSerializationError;
                continue;
            }

            if(result != Status::Ok)
                continue;

            if(!reader.read(response))
            {
                result = Status::PartitionManagerSerializationError;
                continue;
            }

            return Status::Ok;
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        catch(...)
        {
            std::cerr << "error\n";
        }

        result = Status::FailedToConnectToPartitionManager;
    }

    return result;
}


void PoolMonitor::run()
{
    try
    {
        registerNodePool();

        while(!pool.isTerminated())
        {
            std::this_thread::sleep_for(pool.getConfig().getMonitorSleepTime());

            //  Crash if any of the nodes didn't do any processing
            for(size_t i = 0; i < pool.getNodesCount(); i++)
                ASSERT(pool.getNode(i).resetProcessedRoundsSinceLastCheck());

            sendHeartbeat();
        }
    }
    catch(const std::exception& e)
    {
        state = State::failure;
        std::cerr << e.what() << '\n';
        ASSERT(false);
    }
    catch(...)
    {
        state = State::failure;
        ASSERT(false);
    }
}


void PoolMonitor::registerNodePool()
{
    manager::NodePoolRegistrationMessage::Request registerRequest;
    manager::NodePoolRegistrationMessage::Response registerResponse;
    registerRequest.poolInfo = poolInfo;
    TIF(sendMessage(registerRequest, registerResponse));

    sessionId = registerResponse.sessionId;
    lastHeartbeat = TimePoint::now();
    state = State::active;
}


void PoolMonitor::sendHeartbeat()
{
    manager::HeartbeatMessage::Request heartbeatRequest;
    manager::HeartbeatMessage::Response heartbeatResponse;
    heartbeatRequest.poolInfo = poolInfo;
    heartbeatRequest.sessionId = sessionId;

    TIF(sendMessage(heartbeatRequest, heartbeatResponse));

    lastHeartbeat = TimePoint::now();
}

}   //  namespace k2
