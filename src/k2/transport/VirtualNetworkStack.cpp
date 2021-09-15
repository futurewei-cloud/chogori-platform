/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include "VirtualNetworkStack.h"

// third-party
#include <memory>
#include <seastar/core/reactor.hh>
#include <seastar/net/net.hh>

// k2
#include "Log.h"

// determine the packet size we should allocate: mtu - tcp_header_size - ip_header_size - ethernet_header_size
constexpr uint16_t tcpsegsize = seastar::net::hw_features().mtu
                            - seastar::net::tcp_hdr_len_min
                            - seastar::net::ipv4_hdr_len_min
                            - seastar::net::eth_hdr_len;

constexpr uint16_t rrdmasegsize = seastar::rdma::RDMAStack::RCDataSize;

namespace k2 {

VirtualNetworkStack::VirtualNetworkStack() {
    K2LOG_D(log::tx, "ctor");
    registerLowTCPMemoryObserver(nullptr); // install default observer
    registerLowRRDMAMemoryObserver(nullptr); // install default observer
}

VirtualNetworkStack::~VirtualNetworkStack() {
    _lowTCPMemObserver = nullptr;
    K2LOG_D(log::tx, "dtor");
}

seastar::server_socket VirtualNetworkStack::listenTCP(SocketAddress sa, seastar::listen_options opt) {
    K2LOG_D(log::tx, "listen tcp on: {}", sa);
    // TODO For now, just use the engine's global network
    return seastar::engine().net().listen(std::move(sa), std::move(opt));
}

seastar::future<seastar::connected_socket>
VirtualNetworkStack::connectTCP(SocketAddress remoteAddress, SocketAddress sourceAddress) {
    // TODO For now, just use the engine's global network
    return seastar::engine().net().connect(std::move(remoteAddress), std::move(sourceAddress));
}

void VirtualNetworkStack::start(){
    K2LOG_D(log::tx, "start");
}

std::shared_ptr<BinaryAllocator> VirtualNetworkStack::getTCPAllocator() {
    // The seastar stacks don't expose allocation mechanism so we just allocate
    // the binaries in user space

    //NB at some point, we'll have to capture the underlying network stack here in order
    // to pass on allocations. This should be done with weakly_referencable and weak_from_this()

    // TODO: it seems only ipv4 is supported via the seastar's network_stack, so just use ipv6 header size here

    // NB, there is no performance benefit of allocating smaller chunks. Chunks up to 16384 are allocated from
    // seastar pool allocator and overhead is the same regardless of size(~10ns per allocation)
    return Payload::DefaultAllocator(tcpsegsize);
}

void VirtualNetworkStack::registerLowTCPMemoryObserver(LowMemoryObserver_t observer) {
    if (observer == nullptr) {
        _lowTCPMemObserver = [](size_t requiredReleaseBytes) {
            K2LOG_W(log::tx,
                "TCP transport needs {}bytes released, but there is no observer registered", requiredReleaseBytes);
        };
    }
    else {
        //TODO hook this into the underlying TCP provider
        _lowTCPMemObserver = observer;
    }
}

seastar::future<> VirtualNetworkStack::stop() {
    K2LOG_D(log::tx, "stop");
    return seastar::make_ready_future<>();
}

seastar::rdma::RDMAListener VirtualNetworkStack::listenRRDMA() {
    K2LOG_D(log::tx, "listen rrdma");
    return seastar::engine()._rdma_stack->listen();
}

std::unique_ptr<seastar::rdma::RDMAConnection>
VirtualNetworkStack::connectRRDMA(seastar::rdma::EndPoint remoteAddress) {
    return seastar::engine()._rdma_stack->connect(std::move(remoteAddress));
}

std::shared_ptr<BinaryAllocator> VirtualNetworkStack::getRRDMAAllocator() {
    return Payload::DefaultAllocator(rrdmasegsize);
}

void VirtualNetworkStack::registerLowRRDMAMemoryObserver(LowMemoryObserver_t observer) {
    if (observer == nullptr) {
        _lowRRDMAMemObserver = [](size_t requiredReleaseBytes) {
            K2LOG_W(log::tx,
                "RRDMA transport needs {}bytes released, but there is no observer registered", requiredReleaseBytes);
        };
    }
    else {
        //TODO hook this into the underlying RDMA provider
        _lowRRDMAMemObserver = observer;
    }
}

} // k2 namespace
