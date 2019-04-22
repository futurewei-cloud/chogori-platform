//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "VirtualNetworkStack.h"

// third-party
#include <seastar/core/reactor.hh>
#include <seastar/net/net.hh>

// k2tx
#include "Log.h"

namespace k2tx {

VirtualNetworkStack::VirtualNetworkStack() {
    K2DEBUG("ctor");
    RegisterLowTCPMemoryObserver(nullptr); // install default observer
}

VirtualNetworkStack::~VirtualNetworkStack() {
    K2DEBUG("dtor");
}

seastar::server_socket VirtualNetworkStack::ListenTCP(seastar::socket_address sa, seastar::listen_options opt) {
    K2DEBUG("listen tcp" << sa);
    // TODO For now, just use the engine's global network
    return seastar::engine().net().listen(std::move(sa), std::move(opt));
}

seastar::future<seastar::connected_socket>
VirtualNetworkStack::ConnectTCP(seastar::socket_address remoteAddress, seastar::socket_address sourceAddress) {
    // TODO For now, just use the engine's global network
    return seastar::engine().net().connect(std::move(remoteAddress), std::move(sourceAddress));
}

void VirtualNetworkStack::Start(){
    K2DEBUG("start");
}

Allocator_t VirtualNetworkStack::GetTCPAllocator() {
    // The seastar stacks don't expose allocation mechanism so we just allocate
    // the fragments in user space
    return []() {
        //NB at some point, we'll have to capture the underlying network stack here in order
        // to pass on allocations. This should be done with weakly_referencable and weak_from_this()

        // determine the packet size we should allocate: mtu - tcp_header_size - ip_header_size - ethernet_header_size
        // TODO: it seems only ipv4 is supported via the seastar's network_stack, so just use ipv6 header size here
        static const uint16_t tcpsegsize = seastar::net::hw_features().mtu - seastar::net::tcp_hdr_len_min - seastar::net::ipv4_hdr_len_min - seastar::net::eth_hdr_len;
        K2DEBUG("allocating fragment with size=" << tcpsegsize);
        return Fragment(tcpsegsize);
    };
}

void VirtualNetworkStack::RegisterLowTCPMemoryObserver(LowMemoryObserver_t observer) {
    if (observer == nullptr) {
        _lowTCPMemObserver = [](size_t requiredReleaseBytes){
            K2WARN("TCP transport needs memory: "<< requiredReleaseBytes << ", but there is no observer registered");
        };
    }
    else {
        //TODO hook this into the underlying TCP provider
        _lowTCPMemObserver = observer;
    }
}

seastar::future<> VirtualNetworkStack::stop() {
    K2DEBUG("stop");
    return seastar::make_ready_future<>();
}

} // namespace
