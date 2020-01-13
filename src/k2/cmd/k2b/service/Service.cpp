// std
#include <iostream>
#include <array>
#include <atomic>
#include <fstream>
#include <thread>
// boost
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/array.hpp>
#include <boost/asio.hpp>
// seastar
#include <seastar/core/reactor.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics.hh>
// k2:config
#include <k2/config/ConfigLoader.h>
// k2:client
#include <k2/client/Client.h>
// k2:benchmarker
#include <k2/benchmarker/generator/RandomNumberGenerator.h>
#include <k2/benchmarker/KeySpace.h>
#include <k2/benchmarker/Session.h>
#include <k2/benchmarker/proto/k2bdto.pb.h>
// kv module
#include <k2/modules/memkv/server/MemKVModule.h>

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
     payload.write(request);
}

class Context
{
public:
    client::IClient& _rClient;
    std::shared_ptr<config::Config> _pClusterConfig;

    Context(client::IClient& rClient, std::shared_ptr<config::Config> pClusterConfig)
    : _rClient(rClient)
    , _pClusterConfig(pClusterConfig)
    {
        // empty
    }

    std::unique_ptr<Context> newInstance()
    {
        return std::unique_ptr<Context>(new Context(_rClient, _pClusterConfig));
    }
};

class BenchmarkerService: public IApplication<Context>
{
protected:
    std::unique_ptr<Context> _pContext;
    using SessionPtr = std::shared_ptr<Session>;
    using SessionMap = std::map<std::string, SessionPtr>;
    volatile const int _tcpPort = 1300;
    std::shared_ptr<asio::io_service> _pIoService;
    std::shared_ptr<asio::ip::tcp::acceptor> _pAcceptor;
    std::thread _asioThread;
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
                _readySessions.emplace(sessionId, pSession);
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

        return response;
    }

    client::Operation createClientOperation(client::Range range, Payload payload)
    {
        client::Operation operation;
        client::Message message;
        message.content = std::move(payload);
        message.ranges.push_back(std::move(range));
        operation.messages.push_back(std::move(message));

        return operation;
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
            const std::string key = pSession->next();

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

    virtual Duration eventLoop()
    {
        if(!_readySessions.empty()) {
            SessionPtr pSession = _readySessions.begin()->second;
            _readySessions.erase(_readySessions.begin());
            _runningSessions.emplace(pSession->getSessionId(), pSession);
        }

        if(!_runningSessions.empty()) {
            SessionPtr pSession = _runningSessions.begin()->second;
            const int delay = runSession(pSession);
            if(delay < 0) { // we are done
                auto it = _runningSessions.find(pSession->getSessionId());
                if(it!=_runningSessions.end()) {
                    _runningSessions.erase(it);
                }
                _compleatedSessions.emplace(pSession->getSessionId(), pSession);
                printReport(pSession);
            }
            else
            {
                return std::chrono::microseconds(delay); // schedule the next batch after the delay
            }
        }

        // we do not have any active session; take a break before scheduling again
        return 100us;
    }

    virtual void onInit(std::unique_ptr<Context> pContext)
    {
        _pContext = std::move(pContext);
    }

   virtual void onStart()
    {
        registerMetrics();
	    bindPort();
	    listenAsync();

        // start asio service in separate thread otherwise it will be blocked by the k2 client loop
        _asioThread = std::thread([this] {
            _pIoService->run();
        });

        K2INFO("Started K2 Benchmarker Service");
    }

    virtual void onStop()
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
	       response = handleRequest(request);
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

    void printReport(std::shared_ptr<Session> pSession)
    {
        K2INFO("Session report; "
               << "sessionId:" << pSession->getSessionId()
               << ", totalCount:" << pSession->getTargetCount()
               << ", failed:" << pSession->getFailedKeys().size()
               << ", retryCounter:" << pSession->getRetryCounter()
               << ", durationSeconds:" << std::to_string(k2::sec(pSession->getEndTime() - pSession->getStartTime()).count())
        );
    }

    std::vector<SessionPtr> filterSessions(SessionMap& map, std::string type)
    {
        std::vector<SessionPtr> vector;

        for(auto pair : map) {
            if(pair.second->getType() == type) {
                vector.push_back(pair.second);
            }
        }

        return vector;
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
        auto partitions = _pContext->_rClient.getPartitions(range);

        _pContext->_rClient.execute(partitions[0],
            [this, key] (Payload& payload) mutable {
                // populate payload
                makeSetMessage(payload, key, key);
            }
            ,
            [pSession, key](client::IClient& client, client::OperationResult&& result) {
                // prevent compilation warnings
                (void)client;
                if(!result._responses[0].status.is2xxOK()) {
                    K2INFO("client response: " << result._responses[0].status << ", key:" << key);
                    pSession->failed(key);
                }
                else {
                    pSession->success(key);
                }
        });
    }

    void sendKey(const std::string key, SessionPtr pSession)
    {
        _pContext->_rClient.createPayload([this, pSession, key] (client::IClient& rClient, Payload&& payload) {
            try {
                // populate payload
                makeSetMessage(payload, key, key);
                // create operation
                client::Range range = client::Range::singleKey(key);
                client::Operation operation = createClientOperation(std::move(range), std::move(payload));
                // execute operation
                rClient.execute(std::move(operation), [pSession, key](client::IClient& client, client::OperationResult&& result) {
                    // prevent compilation warnings
                    (void)client;
                    if(!result._responses[0].status.is2xxOK()) {
                        K2INFO("client response: " << result._responses[0].status << ", key:" << key);
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

    virtual std::unique_ptr<IApplication> newInstance()
    {
        return std::unique_ptr<IApplication>(new BenchmarkerService());
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

    namespace bpo = boost::program_options;
    bpo::options_description options("K2B Options");

    // get the k2 config from the command line
    options.add_options()
        ("k2config", bpo::value<std::string>(), "k2 configuration file")
        ;

    // parse the command line options
    bpo::variables_map optionsMap;
    bpo::store(bpo::parse_command_line(argc, argv, options), optionsMap);

    std::shared_ptr<config::Config> pK2Config = optionsMap.count("k2config")
        ? config::ConfigLoader::loadConfig(optionsMap["k2config"].as<std::string>())
        : config::ConfigLoader::loadDefaultConfig();

     // initialize the client
    client::ClientSettings settings;
    settings.networkProtocol = "tcp+k2rpc";
    client::Client _client;
    BenchmarkerService service;
    Context context(_client, pK2Config);
    _client.registerApplication(service, context);
    // start the client; will block until stopped
    _client.init(settings, pK2Config);

    return 0;
}
