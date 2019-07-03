//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "K2TXPlatform.h"
#include <seastar/core/app-template.hh> // for app_template
#include "common/Log.h"
#include "common/Constants.h"
#include "transport/IRPCProtocol.h"
#include <sched.h>
#include <common/seastar/SeastarApp.h>
#include <transport/SeastarTransport.h>
#include <boost/scoped_array.hpp>

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
    _header(0),
    _request(std::move(request)),
    _disp(disp) {
        bool ret = _outPayload->getWriter().reserveContiguousStructure(_header);
        assert(ret); //  Always must have space for a response header
    }

    ~AMClientConnection() {}
public:
    //
    //  Send reponse to sender
    //
    PayloadWriter getResponseWriter() override {
        assert(!_responded);
        return _outPayload->getWriter();
    }

    //
    //  Send error to sender
    //
    void sendResponse(Status status, uint32_t code = 0) override {
        assert(!_responded);

        _header->status = status;
        _header->moduleCode = code;
        _header->messageSize = _outPayload->getSize() - txconstants::MAX_HEADER_SIZE - sizeof(ResponseMessage::Header);
        _responded = true;

        _disp.sendReply(std::move(_outPayload), _request);
    }

private:
    bool _responded;
    std::unique_ptr<Payload> _outPayload;
    ResponseMessage::Header* _header;
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
    inPayload->getReader().read(header);
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
        _node(pool.getCurrentNode()), _stopped(true), _dispatcher(dispatcher) {}

public: // distributed<> interface
    void start(seastar::reference_wrapper<NodePoolImpl> pool)
    {
        _stopped = false;

        _dispatcher.local().registerMessageObserver(
                KnownVerbs::PartitionMessages,
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
        _taskProcessorLoop = startTaskProcessor();

        //  Initialized nodes with correct endpoint
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
        _dispatcher.local().registerMessageObserver(KnownVerbs::PartitionMessages, nullptr);
        _dispatcher.local().registerLowTransportMemoryObserver(nullptr);
        return when_all(std::move(_taskProcessorLoop))
            .then_wrapped([](auto&& fut) {
                fut.ignore_ready_future();
                return seastar::make_ready_future<>();
            });
    }
private: // helpers
    seastar::future<> startTaskProcessor()
    {
        return seastar::do_until(
            [this] { return _stopped; },
            [this]
            {
                _node.processTasks();
                return seastar::make_ready_future<>();
            });
    }

private: // fields
    Node& _node;
    bool _stopped;
    seastar::future<> _taskProcessorLoop = seastar::make_ready_future<>();
    k2::RPCDispatcher::Dist_t& _dispatcher;

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

    boost::scoped_array<const char *> argv(new const char *[10]);
    int argc = 0;
    argv[argc++] = "NodePool";
    argv[argc++] = "--poll-mode";
    argv[argc++] = (cpuSetArg.empty() ? "-c" : "--cpuset");
    argv[argc++] = (cpuSetArg.empty() ? nodesCountArg.c_str() : cpuSetArg.c_str());
    argv[argc++] = "-m";
    argv[argc++] = pool.getConfig().getMemorySizeString().c_str();
    if ( isHugePagesEnabled )
    {
        argv[argc++] = "--hugepages";
    }
    if ( isRDMAEnabled )
    {
        argv[argc++] = "--rdma";
        argv[argc++] = pool.getConfig().getRdmaNicId().c_str();
    }
    argv[argc] = nullptr;

    //
    //  Initialize application, transport and service
    //
    SeastarApp app;
    SeastarTransport transport(app, TransportConfig(std::make_unique<NodePoolAddressProvider>(pool), pool.getConfig().isRDMAEnabled()));
    NodePoolService::Distributed service(app, pool, transport);

    //
    //  Start seastar processing loop
    //
    int result = app.run(argc, argv.get());

    K2INFO("Shutdown was successful!");
    return result == 0 ? Status::Ok : Status::SchedulerPlatformStartingFailure;
}


} // namespace k2
