// std
#include <iostream>
#include <chrono>
#include <array>
#include <atomic>
#include <fstream>
#include <thread>
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

namespace asio = boost::asio;

static void makeSetMessage(k2::Payload& payload, std::string key, std::string value)
{
     MemKVModule<>::SetRequest setRequest { std::move(key), std::move(value) };
     auto request = MemKVModule<>::RequestWithType(setRequest);
     payload.getWriter().write(request);
}

class K2Client: public client::Client
{
public:
    K2Client()
    {
        client::PartitionDescription desc;
        PartitionAssignmentId id;
        id.parse("1.1.1");
        desc.nodeEndpoint = "tcp+k2rpc://127.0.0.1:11311";
        desc.id = id;
        client::PartitionMapRange partitionRange;
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
    volatile const int _tcpPort = 1300;
    std::shared_ptr<asio::io_service> _pIoService;
    std::shared_ptr<asio::ip::tcp::acceptor> _pAcceptor;
    std::thread _asioThread;
    K2Client _client;
    Session _session;

public:
    BenchmarkerService()
    : _session(std::move(Session(KeySpace(std::unique_ptr<Generator>(new RandomNumberGenerator()), 10))))
    {
        _pIoService = std::make_shared<asio::io_service>();
        _pAcceptor = std::make_shared<asio::ip::tcp::acceptor>(*_pIoService);
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

    client::Operation createClientOperation(client::Range range, Payload payload)
    {
        client::Operation operation;
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
            client::Range range = client::Range::singleKey("f");
            client::Operation operation = std::move(createClientOperation(std::move(range), std::move(payload)));
            // execute operation
            _client.execute(std::move(operation), [&](client::IClient& client, client::OperationResult&& result) {
                // prevent compilation warnings
                (void)client;
                K2INFO("client response: " << getStatusText(result._responses[0].status));
            });
        });

        return true;
    }

    uint64_t transportLoop(client::IClient& client)
    {
        (void)client;
        runSession(_session);

        return 100000;
    }

    void initK2Client()
    {
        // initialize the client
        client::ClientSettings settings;
        settings.userInitThread = true;
        settings.networkProtocol = "tcp+k2rpc";
        settings.runInLoop = std::bind(&BenchmarkerService::transportLoop, this, std::placeholders::_1);

        _client.init(settings);
    }

    void start()
    {
	    bindPort();
	    listenAsync();

        // start asio service in separate thread otherwise it will be blocked by the k2 client loop
        _asioThread = std::thread([this] {
            _pIoService->run();
        });

        // start k2 client; blocking call
        initK2Client();

        // stop this service
        stop();
    }

    void stop()
    {
        _pIoService->stop();
        _pAcceptor->cancel();
        _pAcceptor->close();
        _asioThread.join();
    }

    void handleAcceptAsync(std::shared_ptr<asio::ip::tcp::socket> pSocket, const boost::system::error_code &socketError)
    {
    	(void)socketError;
	    readSocket(pSocket);

        // listen for the next request
	    listenAsync();
    }

    void listenAsync()
    {
    	std::shared_ptr<asio::ip::tcp::socket> pSocket = std::make_shared<asio::ip::tcp::socket>(*_pIoService);
	    _pAcceptor->async_accept(*pSocket, std::bind(&BenchmarkerService::handleAcceptAsync, this, pSocket, std::placeholders::_1));
    }

    void bindPort()
    {
        K2INFO("Benchmarker listening on port:" << _tcpPort);

        auto endpoint = asio::ip::tcp::endpoint(asio::ip::tcp::v4(), _tcpPort);
       	_pAcceptor.reset(new asio::ip::tcp::acceptor(*_pIoService));
    	_pAcceptor->open(endpoint.protocol());
        _pAcceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
        _pAcceptor->bind(endpoint);
        _pAcceptor->listen();
    }

    void readSocket(std::shared_ptr<asio::ip::tcp::socket> pSocket)
    {
       // read socket
       std::array<char, 1024> readBuffer;
       boost::system::error_code error;
       pSocket->read_some(asio::buffer(readBuffer), error);
       if (error == asio::error::eof) {
	       //break; // Connection closed cleanly by peer.
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
       auto asioBuffer = asio::buffer(output.c_str(), output.size());
       asio::write(*pSocket, asioBuffer, ignored_error);
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
