//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "K2TXPlatform.h"
#include <seastar/core/app-template.hh> // for app_template
#include "common/Log.h"
#include "common/Constants.h"
#include "transport/IRPCProtocol.h"

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
    AMClientConnection(k2::Request request, k2::RPCDispatcher& disp):
    _responded(false),
    _outPayload(request.endpoint.newPayload()),
    _header(0),
    _request(std::move(request)),
    _disp(disp) {
        bool ret = _outPayload->getWriter().getContiguousStructure(_header);
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
        _header->messageSize = _outPayload->getSize()-sizeof(ResponseMessage::Header);
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
NodePoolService::NodePoolService(NodePool& pool, k2::RPCDispatcher::Dist_t& dispatcher):
    _assignmentManager(pool),
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
    _dispatcher.local().registerMessageObserver((Verb)Constants::NodePoolMsgVerbs::PARTITION_MESSAGE, nullptr);
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
    _dispatcher.local().registerMessageObserver((Verb)Constants::NodePoolMsgVerbs::PARTITION_MESSAGE,
            [this](k2::Request& request) mutable {
                K2DEBUG("Dispatching message to AssignmentManager");
                PartitionRequest partitionRequest;
                partitionRequest.message = createPartitionMessage(std::move(request.payload), request.endpoint.getURL());
                partitionRequest.client = std::make_unique<AMClientConnection>(std::move(request), _dispatcher.local());
                _assignmentManager.processMessage(partitionRequest);
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
            _assignmentManager.processTasks();
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
    NodePoolAddressProvider(NodePool& pool): _pool(pool){}
    ~NodePoolAddressProvider(){}
    seastar::socket_address getAddress(int coreID) override {
        NodeEndpointConfig nodeConfig = _pool.getEndpoint(coreID);
        return seastar::make_ipv4_address(nodeConfig.ipv4.address, nodeConfig.ipv4.port);
    }

private:
    NodePool& _pool;
};

Status K2TXPlatform::run(NodePool& pool) {
    // service are constructed starting with the available VirtualNetworkStacks(dpdk receive queues)
    // so that we have a stack on each core. The stack is:
    //  1 VF -> N protocols -> 1 RPCDispatcher -> 1 ServiceHandler.
    // Note how we can have more than one listener per VF so that we can process say UDP and TCP packets at the
    // same time.
    // To build this, we use the seastar distributed<> mechanism, but we extend it so that we
    // limit the pool to the number of network stacks we want to watch. The stacks are configured based on the cmd line options
    k2::VirtualNetworkStack::Dist_t vnet;
    k2::RPCProtocolFactory::Dist_t tcpproto;
    k2::RPCDispatcher::Dist_t dispatcher;
    NodePoolService::Dist_t service;
    NodePoolAddressProvider addrProvider(pool);

    // TODO: So far couldn't find a good way to configure Seastar thorugh app. add_options without using command arguments.
    // Will do more research later and fix it. Now just configure through argument
    std::string nodesCountArg = std::string("-c") + std::to_string(pool.getNodesCount());

    const char* argv[] = { "NodePool", nodesCountArg.c_str(), nullptr };
    int argc = sizeof(argv) / sizeof(*argv) - 1;


    // we are now ready to assemble the running application
    seastar::app_template app;
    auto result = app.run_deprecated(argc, (char**)argv, [&] {
        //auto&& config= app.configuration(); // TODO use this configuration to configure things

        // call the stop() method on each object when we're about to exit. This also deletes the objects
        seastar::engine().at_exit([&] {
            K2INFO("vnet stop");
            return vnet.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("tcpproto stop");
            return tcpproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("dispatcher stop");
            return dispatcher.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("service stop");
            return service.stop();
        });

        return
            // These calls here actually call the constructor of each distributed class, once on
            // each core. This produces thread-local instances in each distributed<>
            // create and wire all objects. We do this by chaining a bunch of
            // futures which all complete pretty much instantly.
            // The application remains running since various components register recurring events
            // There are also some internal components which register recurring events (e.g. metrics http server)
            vnet.start()
            .then([&vnet, &tcpproto, &addrProvider]() {
                K2INFO("start tcpproto");
                return tcpproto.start(k2::TCPRPCProtocol::builder(std::ref(vnet), std::ref(addrProvider)));
            })
            .then([&]() {
                K2INFO("start dispatcher");
                return dispatcher.start();
            })
            .then([&]() {
                K2INFO("start service");
                return service.start(std::ref(pool), std::ref(dispatcher));
            })
            // once the objects have been constructed and wired, they can
            // all perform their startup logic
            .then([&]() {
                K2INFO("Start VNS");
                return vnet.invoke_on_all(&k2::VirtualNetworkStack::start);
            })
            .then([&]() {
                K2INFO("Start tcpproto");
                return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
            })
            .then([&]() {
                K2INFO("RegisterProtocol dispatcher");
                // Could register more protocols here via separate invoke_on_all calls
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
            })
            .then([&]() {
                K2INFO("Start dispatcher");
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
            })
            .then([&]() {
                K2INFO("Start service");
                return service.invoke_on_all(&NodePoolService::start);
        });
    });
    K2INFO("Shutdown was successful!");
    return result == 0 ? Status::Ok : Status::SchedulerPlatformStartingFailure;;
}

} // namespace k2
