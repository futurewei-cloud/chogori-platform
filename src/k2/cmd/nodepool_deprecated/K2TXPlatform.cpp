//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "K2TXPlatform.h"
#include <k2/common/Log.h>
#include <k2/common/TimeMeasure.h>
#include <k2/k2types/Constants.h>
#include <k2/k2types/MessageVerbs.h>
#include <k2/transport/IRPCProtocol.h>
#include <sched.h>
#include <seastar/core/app-template.hh>  // for app_template
#include "SeastarApp.h"
#include "SeastarTransport.h"

namespace k2 {

//
// glue classes used to glue existing AssignmentManger assumptions to message-oriented transport
//

// This class is a copy of the MessageRound class from node/seastar/SeastarPlatform.h
// but it works on top of a message-oriented transport instead of being connection-oriented.
// Its purpose is to present an IClientConnection to the AssignmentManager
class AMClientConnection: public IClientConnection {
public:
    // construct a "connection" to allow a response to be sent to the given request
    AMClientConnection(k2::Request&& request, k2::RPCDispatcher& disp):
    _responded(false),
    _outPayload(request.endpoint.newPayload()),
    _headerPos(_outPayload->getCurrentPosition()),
    _request(std::move(request)),
    _disp(disp)
    {
        _outPayload->skip(sizeof(ResponseMessage::Header));
    }

    ~AMClientConnection() {}
public:
    //
    //  Send reponse to sender
    //
    Payload& getResponsePayload() override {
        assert(!_responded);
        return *_outPayload.get();
    }

    //
    //  Send error to sender
    //
    void sendResponse(Status status, uint32_t code = 0) override {
        assert(!_responded);
        _outPayload->seek(_headerPos);
        _header.status = status;
        _header.moduleCode = code;
        _header.messageSize = _outPayload->getSize() - txconstants::MAX_HEADER_SIZE - sizeof(ResponseMessage::Header);
        _responded = true;
        _outPayload->write(_header);
        _disp.sendReply(std::move(_outPayload), _request);
    }

private:
    bool _responded;
    std::unique_ptr<Payload> _outPayload;
    Payload::PayloadPosition _headerPos;
    ResponseMessage::Header _header;
    k2::Request _request;
    k2::RPCDispatcher& _disp;
};

// Create a new PartitionMessage from the given payload and url
std::unique_ptr<PartitionMessage> createPartitionMessage(std::unique_ptr<Payload> inPayload, const String& url)
{
    if (!inPayload) {
        K2ERROR("Received unexpected message with empty payload from: " << url);
        return nullptr;
    }

    PartitionMessage::Header header;
    inPayload->seek(0);
    inPayload->read(header);
    auto readOffset = sizeof(header);
    auto remainingBytes = header.messageSize;
    assert(readOffset + remainingBytes == inPayload->getSize());
    auto&& buffers = inPayload->release();

    // rewind the first buffer to the offset we need to start reading the payload from
    buffers[0].trim_front(readOffset);

    return std::make_unique<PartitionMessage>(
        header.messageType,
        header.partition,
        String(url),
        Payload(std::move(buffers), remainingBytes)
    );
}

// This class allows us to run a NodePool as a distributed<> service. It essentially wraps
// an AssignmentManager, making sure we have one assignment manager per core
class NodePoolService
{
public: // types
    class Distributed : public seastar::distributed<NodePoolService>
    {
    public:
        Distributed(SeastarApp& server, NodePoolImpl& pool,  SeastarTransport& transport)
        {
            server
                .registerService(*this, std::ref(pool), std::ref(transport.getDispatcher()))
                .startOnCores(*this, seastar::ref(pool))
                .registerInitializer([&]()
                    {
                        pool.completeInitialization();
                        K2INFO("Node Pool successfully initialized.");
                        return seastar::make_ready_future<>();
                    });
        }
    };

public:
    NodePoolService(INodePool& pool, k2::RPCDispatcher::Dist_t& dispatcher) :
        _node(pool.getCurrentNode()), _stopped(true), _dispatcher(dispatcher),
        _nodeTaskProcessor(seastar::reactor::poller::simple([this] { return _node.processTasks(); })) {}

public: // distributed<> interface
    void start(seastar::reference_wrapper<NodePoolImpl> pool)
    {
        _stopped = false;

        _dispatcher.local().registerMessageObserver(
                K2Verbs::PartitionMessages,
                [this](k2::Request&& request) mutable
                {
                    K2DEBUG("Dispatching message to AssignmentManager");
                    PartitionRequest partitionRequest;
                    partitionRequest.message = createPartitionMessage(std::move(request.payload), request.endpoint.getURL());
                    partitionRequest.client = std::make_unique<AMClientConnection>(std::move(request), _dispatcher.local());

                    _node.assignmentManager.processMessage(partitionRequest);
                });

        // TODO need to pass this on to the assignment manager as it may be holding messages around
        _dispatcher.local().registerLowTransportMemoryObserver(nullptr);

        //
        //  Set correct endpoints for all nodes
        //
        std::vector<String> endpoints;
        for(auto endpoint : _dispatcher.local().getServerEndpoints())
            endpoints.push_back(endpoint->getURL());

        pool.get().setCurrentNodeLocationInfo(std::move(endpoints), sched_getcpu());
    }

    seastar::future<> stop()    // required for seastar::distributed interface
    {
        K2INFO("NodePoolService stopping");
        _stopped = true;
        // unregister all observers
        _dispatcher.local().registerMessageObserver(K2Verbs::PartitionMessages, nullptr);
        _dispatcher.local().registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

private: // fields
    Node& _node;
    bool _stopped;
    k2::RPCDispatcher::Dist_t& _dispatcher;
    seastar::reactor::poller _nodeTaskProcessor;

private: // don't need
    NodePoolService() = delete;
    DISABLE_COPY_MOVE(NodePoolService)
}; // class NodePoolService

//
//  Provide ports for TCP transports
//
class NodePoolAddressProvider: public k2::IAddressProvider {
public:
    NodePoolAddressProvider(INodePool& pool): _pool(pool){}
    ~NodePoolAddressProvider(){}
    seastar::socket_address getAddress(int coreID) const override {
        const NodeEndpointConfig& nodeConfig = _pool.getNode(coreID).getEndpoint();
        return seastar::make_ipv4_address(nodeConfig.ipv4.address, nodeConfig.ipv4.port);
    }
private:
    INodePool& _pool;
};

Status K2TXPlatform::run(NodePoolImpl& pool)
{
    // TODO: So far couldn't find a good way to configure Seastar thorugh app. add_options without using command arguments.
    // Will do more research later and fix it. Now just configure through argument
    std::string nodesCountArg = std::to_string(pool.getNodesCount());
    std::string cpuSetArg = pool.getConfig().getCpuSetString();
    bool isRDMAEnabled = pool.getConfig().isRDMAEnabled();
    bool isHugePagesEnabled = pool.getConfig().isHugePagesEnabled();
    std::string nicId = pool.getConfig().getRdmaNicId();

    std::vector<const char *> argv;
    argv.push_back("NodePool");
    argv.push_back("--poll-mode");
    if ( cpuSetArg.empty() )
    {
        argv.push_back( "-c" );
        argv.push_back( nodesCountArg.c_str() );
    }
    else
    {
        argv.push_back( "--cpuset" );
        argv.push_back( cpuSetArg.c_str() );
    }
    if ( !pool.getConfig().getMemorySizeString().empty() )
    {
        argv.push_back( "-m" );
        argv.push_back( pool.getConfig().getMemorySizeString().c_str() );
    }
    if ( isHugePagesEnabled )
    {
        argv.push_back( "--hugepages" );
    }
    if ( isRDMAEnabled )
    {
        argv.push_back( "--rdma" );
        argv.push_back( nicId.c_str() );
    }
    argv.push_back( nullptr );

    //
    //  Initialize application, transport and service
    //
    SeastarApp app;
    // TODO: set the prometheus port via configuration
    app.registerService(_prometheus, _defaultPrometheusPort, std::ref("K2 node pool metrics"), std::ref("k2_node_pool"));
    SeastarTransport transport(app, TransportConfig(std::make_unique<NodePoolAddressProvider>(pool), pool.getConfig().isRDMAEnabled()));
    NodePoolService::Distributed service(app, pool, transport);

    //
    //  Start seastar processing loop
    //
    int result = app.run(argv.size()-1, argv.data());

    K2INFO("Shutdown was successful!");
    return result == 0 ? Status::S200_OK() : Status::S500_Internal_Server_Error("Error running node pool");
}


} // namespace k2
