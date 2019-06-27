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
std::unique_ptr<PartitionMessage> createPartitionMessage(std::unique_ptr<Payload> inPayload, const String& url) {
    if (!inPayload) {
        K2ERROR("Received unexpected message with empty payload from: " << url);
        return nullptr;
    }
    PartitionMessage::Header header;
    inPayload->getReader().read(&header, sizeof(header));
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

//
// end Glue classes
//
NodePoolService::NodePoolService(INodePool& pool, k2::RPCDispatcher::Dist_t& dispatcher):
    _node(pool.getCurrentNode()),
    _stopped(true),
    _dispatcher(dispatcher) {
    K2DEBUG("NodePoolService constructed")
}

NodePoolService::~NodePoolService() {
    K2DEBUG("NodePoolService destructed")
}

// required for seastar::distributed interface
seastar::future<> NodePoolService::stop() {
    K2INFO("NodePoolService stopping");
    _stopped = true;
    // unregistar all observers
    _dispatcher.local().registerMessageObserver(KnownVerbs::PartitionMessages, nullptr);
    _dispatcher.local().registerLowTransportMemoryObserver(nullptr);
    return when_all(std::move(_taskProcessorLoop))
        .then_wrapped([](auto&& fut) {
            fut.ignore_ready_future();
            return seastar::make_ready_future<>();
        });
}

void NodePoolService::start() {
    K2INFO("NodePoolService starting");
    _stopped = false;

    _dispatcher.local().registerMessageObserver(KnownVerbs::PartitionMessages,
            [this](k2::Request&& request) mutable {
                K2DEBUG("Dispatching message to AssignmentManager");
                PartitionRequest partitionRequest;
                partitionRequest.message = createPartitionMessage(std::move(request.payload), request.endpoint.getURL());
                partitionRequest.client = std::make_unique<AMClientConnection>(std::move(request), _dispatcher.local());
                _node.assignmentManager.processMessage(partitionRequest);
            });

    // TODO need to pass this on to the assignment manager as it may be holding messages around
    _dispatcher.local().registerLowTransportMemoryObserver(nullptr);
    _taskProcessorLoop = startTaskProcessor();
}

seastar::future<> NodePoolService::startTaskProcessor() {
    return seastar::do_until(
        [this] {
            return _stopped; // break loop if we're stopped
        },
        [this] {
            _node.processTasks();
            return seastar::make_ready_future<>();
        }
    ).handle_exception([this] (std::exception_ptr eptr) {
        K2ERROR("Ignoring assignment manager exception: " << eptr);
        return seastar::make_ready_future<>();
    });
}

K2TXPlatform::K2TXPlatform() {
}

K2TXPlatform::~K2TXPlatform() {
}

class NodePoolAddressProvider: public k2::IAddressProvider {
public:
    NodePoolAddressProvider(INodePool& pool): _pool(pool){}
    ~NodePoolAddressProvider(){}
    seastar::socket_address getAddress(int coreID) override {
        const NodeEndpointConfig& nodeConfig = _pool.getNode(coreID).getEndpoint();
        return seastar::make_ipv4_address(nodeConfig.ipv4.address, nodeConfig.ipv4.port);
    }

private:
    INodePool& _pool;
};

class NodesInitializer
{
public:
    void initializeNodes(seastar::reference_wrapper<NodePoolImpl> pool, seastar::reference_wrapper<RPCDispatcher::Dist_t> dispatcher)
    {
        std::vector<String> endpoints;
        for(auto endpoint : dispatcher.get().local().getServerEndpoints())
            endpoints.push_back(endpoint->getURL());

        pool.get().setCurrentNodeLocationInfo(std::move(endpoints), sched_getcpu());
    }

    void start() {}
    seastar::future<> stop() { return seastar::make_ready_future<>(); }
};

Status K2TXPlatform::run(NodePoolImpl& pool)
{
    // TODO: So far couldn't find a good way to configure Seastar thorugh app. add_options without using command arguments.
    // Will do more research later and fix it. Now just configure through argument
    std::string nodesCountArg = std::to_string(pool.getNodesCount());
    std::string cpuSetArg = pool.getConfig().getCpuSetString();

    const char* argv[] = { "NodePool",
                           "--poll-mode",
                           (cpuSetArg.empty() ? "-c" : "--cpuset"),
                           (cpuSetArg.empty() ? nodesCountArg.c_str() : cpuSetArg.c_str()),
                           nullptr };
    int argc = sizeof(argv) / sizeof(*argv) - 1;

    // we are now ready to assemble the running application
    SeastarApp server;
    NodePoolAddressProvider addrProvider(pool);
    SeastarTransport transport(server, addrProvider);
    NodePoolService::Dist_t service;
    seastar::distributed<NodesInitializer> nodesInitializer;

    server
        .registerService(service, std::ref(pool), std::ref(transport.getDispatcher()))
        .registerService(nodesInitializer)
        .registerInitializer([&]()
            {
                K2INFO("Start service");
                return service.invoke_on_all(&NodePoolService::start);
            })
        .registerInitializer([&]()
            {
                K2INFO("Initialize nodes");
                return nodesInitializer.invoke_on_all(&NodesInitializer::initializeNodes, seastar::ref(pool), seastar::ref(transport.getDispatcher()));
            })
        .registerInitializer([&]()
            {
                K2INFO("Complete initialization of the Node Pool");
                pool.completeInitialization();
                return seastar::make_ready_future<>();
            });

    int result = server.run(argc, argv);

    K2INFO("Shutdown was successful!");
    return result == 0 ? Status::Ok : Status::SchedulerPlatformStartingFailure;
}


} // namespace k2
