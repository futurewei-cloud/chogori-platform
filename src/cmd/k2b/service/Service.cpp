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
// seastar
#include <seastar/core/reactor.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics.hh>
// k2:client
#include <client/lib/Client.h>
// k2:benchmarker
#include <benchmarker/generator/RandomNumberGenerator.h>
#include <benchmarker/KeySpace.h>
#include <benchmarker/Session.h>
#include "benchmarker/proto/k2bdto.pb.h"
// kv module
#include "node/module/MemKVModule.h"

namespace k2
{

namespace benchmarker
{

namespace asio = boost::asio;
namespace metrics = seastar::metrics;

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
        PartitionDescription desc;
        PartitionAssignmentId id;
        id.parse("1.1.1");
        desc.nodeEndpoint = "rrdma+k2rpc://[fe80::9a03:9bff:fe89:13ba]:285";
        desc.id = id;
        PartitionRange partitionRange;
        partitionRange.lowKey = "";
        partitionRange.highKey = "";
        desc.range = partitionRange;
        _partitionMap.map.insert(desc);

        /*desc.nodeEndpoint = "tcp+k2rpc://127.0.0.1:12345";
        id.parse("2.1.1");
        desc.id = id;
        partitionRange.lowKey = "z";
        partitionRange.highKey = "";
        desc.range = partitionRange;
        _partitionMap.map.insert(desc);*/
    }
};

class BenchmarkerService
{
using SessionPtr = std::shared_ptr<Session>;
using SessionMap = std::map<std::string, SessionPtr>;

private:
    volatile const int _tcpPort = 1300;
    std::shared_ptr<asio::io_service> _pIoService;
    std::shared_ptr<asio::ip::tcp::acceptor> _pAcceptor;
    std::thread _asioThread;
    K2Client _client;
    metrics::metric_groups _metricGroups;
    std::map<std::string, SessionPtr> _readySessions;
    std::map<std::string, SessionPtr> _runningSessions;
    std::map<std::string, SessionPtr> _compleatedSessions;

public:
    BenchmarkerService()
    {
        _pIoService = std::make_shared<asio::io_service>();
        _pAcceptor = std::make_shared<asio::ip::tcp::acceptor>(*_pIoService);
    }

    k2bdto::Response handleRequest(k2bdto::Request& request)
    {
        std::string tmp;
        request.SerializeToString(&tmp);
        k2bdto::Response response;

        if(request.operation().type() == k2bdto::Request_Operation_Type_PRIME) {
            const uint64_t count = request.operation().load().count();
            if(count <= 0) {
                response.set_status(k2bdto::Response_Status_ERROR);
                response.set_message("Missing count!");
            }
            else {
                std::string sessionId = std::to_string(request.sequenceid());
                SessionPtr pSession = std::make_shared<Session>(sessionId, k2bdto::Request_Operation_Type_Name(request.operation().type()), KeySpace(std::unique_ptr<Generator>(new RandomNumberGenerator()), count));
                pSession->setTargetCount(request.operation().load().count());
                _readySessions.insert(std::move(std::pair<std::string, SessionPtr>(sessionId, pSession)));
                response.set_uuid(sessionId);
                response.set_status(k2bdto::Response_Status_OK);

                K2INFO("Started session;" << pSession->getType() << " uuid:" << pSession->getSessionId() << ", keyCount:" << pSession->getTargetCount())
            }
        }
        else {
            response.set_status(k2bdto::Response_Status_ERROR);
            response.set_message("Invalid operation!");
        }

        response.set_sequenceid(request.sequenceid());
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

    //
    // Return true if the session should be scheduled again, false otherwise.
    //
    int runSession(SessionPtr pSession)
    {
        const int penalty = 5; // microseconds
        const int pipeline = 9;

        for(int i=0; i<pipeline; i++) {
            if(pSession->isDone()) {

               return -1; // we are done
            }

            const int delay = (pipeline-i) * penalty;
            const std::string key = std::move(pSession->next());
            
            try {
                if(key.empty()) {
                    K2INFO("Session iterator exhausted; failedKeys:" << pSession->getFailedKeys().size());

                    return delay; // we do not have more keys to send out, but we need to wait for the ones in-flight to complete
                }

                sendKeyOneShot(key, pSession);
            }
            catch(std::exception& e) { // the client could be busy; retry
                pSession->retry(key);

                return delay; // delay the next batch based on the pending requests
            }
        }

        return 0; // all messages were send
    }

    uint64_t transportLoop(client::IClient& client)
    {
        (void)client;

        if(!_readySessions.empty()) {
            SessionPtr pSession = _readySessions.begin()->second;
            _readySessions.erase(_readySessions.begin());
            _runningSessions.insert(std::move(std::pair<std::string, SessionPtr>(pSession->getSessionId(), pSession)));
        }

        if(!_runningSessions.empty()) {
            SessionPtr pSession = _runningSessions.begin()->second;
            const int delay = runSession(pSession);
            if(delay < 0) { // we are done
                auto it = _runningSessions.find(pSession->getSessionId());
                if(it!=_runningSessions.end()) {
                    _runningSessions.erase(it);
                }
                _compleatedSessions.insert(std::move(std::pair<std::string, SessionPtr>(pSession->getSessionId(), pSession)));
                K2INFO(createReport(pSession));
            }
            else
            {
                return delay; // schedule the next batch after the delay
            }
        }

        // we do not have any active session; take a break before scheduling again
        return 100;
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
        registerMetrics();
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
               response.set_message("error parsing request!");
	       }
       }

       // send back response
       boost::system::error_code ignored_error;
       std::string output;
       response.SerializeToString(&output);
       auto asioBuffer = asio::buffer(output.c_str(), output.size());
       asio::write(*pSocket, asioBuffer, ignored_error);
    }

    std::string createReport(std::shared_ptr<Session> pSession)
    {
        const std::time_t startTime = std::chrono::system_clock::to_time_t(pSession->getStartTime());
        const std::time_t endTime = std::chrono::system_clock::to_time_t(pSession->getEndTime());
        std::stringstream report;
        report << "Session report; "
               << "sessionId:" << pSession->getSessionId()
               << ", totalCount:" << pSession->getTargetCount()
               << ", failed:" << pSession->getFailedKeys().size()
               << ", retryCounter:" << pSession->getRetryCounter()
               << ", durationSeconds:" << std::to_string(std::chrono::duration_cast<std::chrono::seconds>(pSession->getEndTime() - pSession->getStartTime()).count())
               << ", startTime:" << std::ctime(&startTime)
               << ", endTime:" << std::ctime(&endTime)
               << std::endl;

        return std::move(report.str());
    }

    std::vector<SessionPtr> filterSessions(SessionMap& map, std::string type)
    {
        std::vector<SessionPtr> vector;

        for(auto pair : map) {
            if(pair.second->getType() == type) {
                vector.push_back(pair.second);
            }
        }

        return std::move(vector);
    }

    void registerMetrics()
    {
        std::vector<metrics::label_instance> labels;
        std::vector<metrics::label_instance> primeLabels(labels);
        primeLabels.push_back(metrics::label_instance("operation", "prime"));

        _metricGroups.add_group("benchmarker", {
            metrics::make_counter("running_sessions", [this] { return _runningSessions.size();}, metrics::description("Count of running sessions"), labels),
            metrics::make_counter("ready_sessions", [this] { return _readySessions.size();}, metrics::description("Count of ready sessions"), labels),
            metrics::make_counter("compleated_sessions", [this] { return _compleatedSessions.size();}, metrics::description("Count of compleated sessions"), labels),
            metrics::make_queue_length("prime_sessions_published_key_size", [this] {
                int count = 0;
                auto sessions = filterSessions(_runningSessions, "PRIME");
                for(auto pSession: sessions) {
                    count += pSession->getPendingCounter();
                }

                return count;
            }, metrics::description("Count of keys for all prime sessions"), primeLabels),
            metrics::make_histogram("prime_sessions_success_latency", [this] {
                auto sessions = filterSessions(_runningSessions, "PRIME");
                if(sessions.size() > 0) {
                    return sessions[0]->getLatencyHistogram();
                }

                return seastar::metrics::histogram();
            }, metrics::description("Latency of success publish keys"), primeLabels),
        });
    }

    void sendKeyOneShot(const std::string& key, SessionPtr pSession) {
        client::Range range = client::Range::singleKey(key);
        auto partitions = std::move(_client.getPartitions(range));

        _client.execute(partitions[0],
            [this, key] (Payload& payload) mutable {
                // populate payload
                makeSetMessage(payload, key, key);
            }
            ,
            [pSession, key](client::IClient& client, client::OperationResult&& result) {
                // prevent compilation warnings
                (void)client;
                if(result._responses[0].status != Status::Ok) {
                    K2INFO("client response: " << getStatusText(result._responses[0].status) << ", key:" << key);
                    pSession->failed(key);
                }
                else {
                    pSession->success(key);
                }
        });
    }

    void sendKey(const std::string key, SessionPtr pSession)
    {
        _client.createPayload([this, pSession, key] (Payload&& payload) {
            try {
                // populate payload
                makeSetMessage(payload, key, key);
                // create operation
                client::Range range = client::Range::singleKey(key);
                client::Operation operation = std::move(createClientOperation(std::move(range), std::move(payload)));
                // execute operation
                _client.execute(std::move(operation), [pSession, key](client::IClient& client, client::OperationResult&& result) {
                    // prevent compilation warnings
                    (void)client;
                    if(result._responses[0].status != Status::Ok) {
                        K2INFO("client response: " << getStatusText(result._responses[0].status) << ", key:" << key);
                        pSession->failed(key);
                    }
                    else {
                        pSession->success(key);
                    }
                });
            }
            catch(std::exception& e) {
                // the client could be busy; retry
                pSession->retry(key);
                K2WARN("Exception thrown during execution; exception:" << e.what());
            }
        });
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
