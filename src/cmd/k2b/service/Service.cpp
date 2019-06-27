// std
#include <iostream>
#include <chrono>
#include <array>
#include <atomic>
#include <fstream>
// boost
#include "boost/program_options.hpp"
#include "boost/filesystem.hpp"
#include <boost/array.hpp>
#include <boost/asio.hpp>
// k2:client
#include <client/lib/Client.h>
// benchmarker
#include <benchmarker/generator/RandomNumberGenerator.h>
#include <benchmarker/KeySpace.h>
#include <benchmarker/Session.h>
#include "benchmarker/proto/k2bdto.pb.h"
// KV module
#include "node/module/MemKVModule.h"

namespace k2
{

namespace benchmarker
{

using namespace k2;
using namespace k2::client;
using namespace k2::benchmarker;
using boost::asio::ip::tcp;

static void makeSetMessage(k2::Payload& payload, std::string key, std::string value)
{
     MemKVModule<>::SetRequest setRequest { std::move(key), std::move(value) };
     auto request = MemKVModule<>::RequestWithType(setRequest);
     payload.getWriter().write(request);
}

class K2Client: public Client
{
public:
    K2Client()
    {
        PartitionDescription desc;
        PartitionAssignmentId id;
        id.parse("1.1.1");
        desc.nodeEndpoint = "tcp+k2rpc://127.0.0.1:11311";
        desc.id = id;
        PartitionMapRange partitionRange;
        partitionRange.lowKey = "d";
        partitionRange.highKey = "f";
        desc.range = partitionRange;
        _partitionMap.map.insert(desc);

        desc.nodeEndpoint = "tcp+k2rpc://127.0.0.1:12345";
        id.parse("2.1.1");
        desc.id = id;
        partitionRange.lowKey = "f";
        partitionRange.highKey = "";
        desc.range = partitionRange;
        _partitionMap.map.insert(desc);
    }
};

class BenchmarkerService
{
private:
    const int tcpPort = 1300;
    std::atomic_bool _stopFlag;
    boost::asio::io_service _ioService;
    boost::asio::ip::tcp::acceptor _acceptor;
    K2Client _client;
    std::thread _socketThread;
    Session _session;

public:
    BenchmarkerService()
    : _stopFlag(false)
    , _acceptor(boost::asio::ip::tcp::acceptor(_ioService))
    , _session(std::move(Session(KeySpace(std::unique_ptr<Generator>(new RandomNumberGenerator()), 10))))
    {
       // empty
    }

    k2bdto::Response handleRequest(k2bdto::Request& request)
    {
        std::string tmp;
        request.SerializeToString(&tmp);

        if(request.operation().type() == k2bdto::Request_Operation_Type_PRIME) {
            _session._it = std::move(_session._keySpace.begin());
        }

        k2bdto::Response response;
        response.set_sequenceid(request.sequenceid());
        response.set_status(k2bdto::Response_Status_OK);
        response.SerializeToString(&tmp);

        return std::move(response);
    }

    Operation createOperation(Range range, Payload payload)
    {
        Operation operation;
        client::Message message;
        message.content = std::move(payload);
        message.ranges.push_back(std::move(range));
        operation.messages.push_back(std::move(message));

        return std::move(operation);
    }

    bool runSession(Session& session)
    {
        if(session._it == session._keySpace.end()) {
            return false; // we are done
        }

        _client.createPayload([this] (Payload&& payload) {
            // create keys
            std::string key = std::move(*_session._it);
            ++_session._it;
            std::string value = std::move(*_session._it);
            ++_session._it;
            K2INFO("Sending key:" << key << " value: " << value);
            // populate payload
            makeSetMessage(payload, std::move(key), std::move(value));
            // create operation
            Range range = Range::singleKey("f");
            Operation operation = std::move(createOperation(std::move(range), std::move(payload)));
            // execute operation
            _client.execute(std::move(operation), [&](IClient& client, OperationResult&& result) {
                // prevent compilation warnings
                (void)client;
                K2INFO("client response: " << getStatusText(result._responses[0].status));
            });
        });

        return true;
    }

    uint64_t transportLoop()
    {
        runSession(_session);

        return 100000;
    }

    void start()
    {
        _socketThread = std::thread([this] {
            listen();
        });

        // initialize the client
        ClientSettings settings;
        settings.userInitThread = true;
        settings.networkProtocol = "tcp+k2rpc";
        settings.runInLoop = ([this] (k2::client::IClient& client) {
            (void)client;
            return transportLoop();
        });
        // start client
        _client.init(settings);

        stop();
    }

    void stop()
    {
        _stopFlag = true;
        _acceptor.cancel();
        _acceptor.close();
        _socketThread.join();
    }

    void listen()
    {
        K2INFO("Listening on port:" << tcpPort);

         auto endpoint = tcp::endpoint(tcp::v4(), tcpPort);
        _acceptor.open(endpoint.protocol());
        _acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
        _acceptor.bind(endpoint);
        _acceptor.listen();

        try {
            // keep listening until stopped
            for(;!_stopFlag;) {
                tcp::socket socket(_ioService);
                _acceptor.accept(socket);
                // read socket
                std::array<char, 1024> readBuffer;
                boost::system::error_code error;
                socket.read_some(boost::asio::buffer(readBuffer), error);
                if (error == boost::asio::error::eof) {
                    break; // Connection closed cleanly by peer.
                } else if (error) {
                    throw boost::system::system_error(error); // Some other error.
                }

                k2bdto::Response response;
                // parse request
                try {
                    std::string inputString(readBuffer.data());
                    k2bdto::Request request;
                    request.ParseFromString(inputString);
                    response = std::move(handleRequest(request));
                }
                catch(...) {
                    K2ERROR("Exception thrown!");

                    std::exception_ptr pException = std::current_exception();
                    try {
                        std::rethrow_exception(pException);
                    }
                    catch(const std::exception& e) {
                        response.set_status(k2bdto::Response_Status_ERROR);
                        response.set_message(e.what());
                    }
                    catch(...) {
                        response.set_status(k2bdto::Response_Status_ERROR);
                    }
                }

                // send back response
                boost::system::error_code ignored_error;
                std::string output;
                response.SerializeToString(&output);
                auto asioBuffer = boost::asio::buffer(output.c_str(), output.size());
                boost::asio::write(socket, asioBuffer, ignored_error);
            }
        }
        catch(std::exception& e) {
            K2ERROR(e.what());
        }
    }

}; // BenchmarkerService class

}; // benchmarker namespace

}; // k2 namespace

//
// K2 Benchmarker entry point.
//
int main(int argc, char** argv)
{
    using namespace k2;
    using namespace k2::benchmarker;

    BenchmarkerService service;
    service.start();

    (void)argc;
    (void)argv;

    return 0;
}
