#pragma once

#include "SeastarApp.h"
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RRDMARPCProtocol.h>
#include <k2/transport/TCPRPCProtocol.h>
#include <k2/transport/RPCProtocolFactory.h>
#include <k2/transport/VirtualNetworkStack.h>

namespace k2
{
class TransportConfig
{
    std::unique_ptr<IAddressProvider> tcpAddrProvider;
    bool tcpEnabled;
    bool rdmaEnabled;
public:
    TransportConfig(bool tcpEnabled = true, bool rdmaEnabled = false) : tcpEnabled(tcpEnabled), rdmaEnabled(rdmaEnabled)
    {
        assert(tcpEnabled || rdmaEnabled);
    }

    TransportConfig(std::unique_ptr<IAddressProvider> tcpAddrProvider, bool rdmaEnabled = false) :
        tcpAddrProvider(std::move(tcpAddrProvider)), tcpEnabled(true), rdmaEnabled(rdmaEnabled) { }

    bool isTCPEnabled() const { return tcpEnabled; }
    bool isTCPServer() const { return (bool)tcpAddrProvider; }
    bool isRDMAEnabled() const { return rdmaEnabled; }

    IAddressProvider& getTCPAddressProvider() const
    {
        assert(isTCPServer());
        return *tcpAddrProvider;
    }

    DEFAULT_MOVE(TransportConfig);
};

class SeastarTransport
{
protected:
    // service are constructed starting with the available VirtualNetworkStacks(dpdk receive queues)
    // so that we have a stack on each core. The stack is:
    //  1 VF -> N protocols -> 1 RPCDispatcher -> 1 ServiceHandler.
    // Note how we can have more than one listener per VF so that we can process say UDP and TCP packets at the
    // same time.
    // To build this, we use the seastar distributed<> mechanism, but we extend it so that we
    // limit the pool to the number of network stacks we want to watch. The stacks are configured based on the cmd line options
    TransportConfig config;
    VirtualNetworkStack::Dist_t vnet;
    RPCProtocolFactory::Dist_t tcpProto;
    RPCProtocolFactory::Dist_t rdmaProto;
    RPCDispatcher::Dist_t dispatcher;

    class ProtocolInitializer
    {
    protected:
        std::vector<std::tuple<RPCProtocolFactory::Dist_t*, RPCProtocolFactory::BuilderFunc_t, const char*>> protocols;
    public:
        void add(RPCProtocolFactory::Dist_t& protocol, RPCProtocolFactory::BuilderFunc_t&& builder, const char* protoName)
        {
            protocols.emplace_back(&protocol, std::move(builder), protoName);
        }

        //  Should be called after Virtual Network Stack service registration and before dispatcher registration
        void init(SeastarApp& app)
        {
            for(auto& p : protocols)
                app.registerService(*std::get<0>(p), std::move(std::get<1>(p)));
        }

        void startAndRegister(SeastarApp& app, RPCDispatcher::Dist_t& dispatcher)
        {
            for(auto& p : protocols)
            {
                RPCProtocolFactory::Dist_t& proto = *std::get<0>(p);
                const char* name = std::get<2>(p);
                app
                    .startOnCores(proto)
                    .registerInitializer([&proto, &dispatcher]()
                    {
                        return dispatcher.invoke_on_all(&RPCDispatcher::registerProtocol, seastar::ref(proto));
                    }).registerInfoLog(std::string("SeastarTransport: protocol ") + name + " successfully initialized.");
            }
        }
    };
public:
    RPCDispatcher::Dist_t& getDispatcher() { return dispatcher; }

    SeastarTransport(SeastarApp& app, TransportConfig&& configuration) : config(std::move(configuration))
    {
        //
        //  Set protocols that we are going to use
        //
        ProtocolInitializer protocols;
        if(config.isTCPEnabled())
        {
            protocols.add(
                tcpProto,
                config.isTCPServer() ?
                    TCPRPCProtocol::builder(std::ref(vnet), std::ref(config.getTCPAddressProvider())) :
                    TCPRPCProtocol::builder(std::ref(vnet)),
                "TCP");
        }

        if(config.isRDMAEnabled())
            protocols.add(rdmaProto, RRDMARPCProtocol::builder(std::ref(vnet)), "RDMA");

        //
        //  Register Virtual Network Stack, protocols and dispatcher
        //
        app.registerService(vnet);

        protocols.init(app);

        app.registerService(dispatcher).startOnCores(vnet);

        protocols.startAndRegister(app, dispatcher);

        app.startOnCores(dispatcher).registerInfoLog("SeastarTransport: successfully initialized.");
    }
};

}  //  namespace k2
