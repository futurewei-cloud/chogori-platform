#pragma once

#include "common/seastar/SeastarApp.h"

namespace k2
{
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
    VirtualNetworkStack::Dist_t vnet;
    RPCProtocolFactory::Dist_t tcpproto;
    RPCDispatcher::Dist_t dispatcher;
    NodePoolService::Dist_t service;
public:
    SeastarTransport(SeastarApp& app, IAddressProvider& addrProvider)
    {
        app
            .registerService(vnet)
            .registerService(tcpproto, k2::TCPRPCProtocol::builder(std::ref(vnet), std::ref(addrProvider)))
            .registerService(dispatcher)
            .registerInitializer([&]()
                {
                    K2INFO("SeastarTransport: Start Virtual Network Stack");
                    return vnet.invoke_on_all(&k2::VirtualNetworkStack::start);
                })
            .registerInitializer([&]()
                {
                    K2INFO("SeastarTransport: Start TCP protocol factory");
                    return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
            .registerInitializer([&]()
                {
                    K2INFO("SeastarTransport: Register TCP with dispatcher");
                    // Could register more protocols here via separate invoke_on_all calls
                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
                })
            .registerInitializer([&]()
                {
                    K2INFO("SeastarTransport: Start dispatcher");
                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
                });
    }

    RPCDispatcher::Dist_t& getDispatcher() { return dispatcher; }
};

}  //  namespace k2