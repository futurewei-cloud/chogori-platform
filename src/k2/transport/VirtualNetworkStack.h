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

#pragma once

// third-party
#include <seastar/core/distributed.hh> // distributed<> stuff
#include <seastar/net/api.hh> // socket/network stuff
#include <seastar/core/future.hh> // future stuff
#include <seastar/net/rdma.hh>

// k2
#include <k2/common/Common.h>
#include "BaseTypes.h"

namespace k2 {

// This class allows access to the proper VirtualNetworkStack(SR-IOV virtualized NIC) on a given core
// This class should be used as a distributed<> container
// TODO This class should allow for registering providers for different functions. For example
// creating a tcp server socket may be done via the dpdk+tcp stack, while creating a udp channel
// is satisfied by a posix stack.
// For now, this is a forward interface which is coded to work with seastar's global network stack
class VirtualNetworkStack{

public: // types
    // Distributed version of the class
    typedef seastar::distributed<VirtualNetworkStack> Dist_t;

public: // lifecycle
    // Constructor.
    VirtualNetworkStack();

    // Destructor
    ~VirtualNetworkStack();

public: // TCP API
    // Create a server(listening) TCP socket on the local VF. The socket is constructed on the
    // given address/port with the given listen_options(e.g. reuse port)
    // It is up to caller to call abort_listen if the socket should be closed.
    seastar::server_socket listenTCP(SocketAddress sa, seastar::listen_options opt);

    // Create a socket to connect to a given remote address. Optionally, it binds locally to the given sourceAddress
    // It is up to caller to shutdown the input/output when the socket should be closed
    seastar::future<seastar::connected_socket> connectTCP(SocketAddress remoteAddress, SocketAddress sourceAddress={});

    // Create a binary allocator from the TCP provider
    std::shared_ptr<BinaryAllocator> getTCPAllocator();

    // registerLowTCPMemoryObserver allows the user to register a observer which will be called when
    // the TCP stack becomes low on memory and requires the application to release some buffers back.
    // The call is level triggered at the end of every polling cycle when the TCP transport detected that
    // its memory is running low. The user is called with is required total number of bytes that should be released.
    //
    // The intended use case here is for applications which hold on to Payloads for long periods of time.
    // These applications should register themselves here, and when called should release enough Payloads to satisfy
    // the requiredNumberOfBytes parameter in their callback.
    void registerLowTCPMemoryObserver(LowMemoryObserver_t observer);

public: // UDP API
    // TODO add UDP support

public: // RDMA API
    // Create a server(listening) RRDMA socket on the local interface.
    seastar::rdma::RDMAListener listenRRDMA();

    // Create an RRDMA connection to connect to a given remote address.
    std::unique_ptr<seastar::rdma::RDMAConnection> connectRRDMA(seastar::rdma::EndPoint remoteAddress);

    // Create a binary allocator from the RRDMA provider
    std::shared_ptr<BinaryAllocator> getRRDMAAllocator();

    // RegisterLowRRDMAMemoryObserver allows the user to register a observer which will be called when
    // the RRDMA stack becomes low on memory and requires the application to release some buffers back.
    // The call is level triggered at the end of every polling cycle when the RRDMA transport detected that
    // its memory is running low. The user is called with is required total number of bytes that should be released.
    //
    // The intended use case here is for applications which hold on to Payloads for long periods of time.
    // These applications should register themselves here, and when called should release enough Payloads to satisfy
    // the requiredNumberOfBytes parameter in their callback.
    void registerLowRRDMAMemoryObserver(LowMemoryObserver_t observer);

public: // distributed<> interface
    // Should be called by user when all distributed objects have been created
    void start();

    // called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    seastar::future<> stop();

private: // fields
    LowMemoryObserver_t _lowTCPMemObserver;
    LowMemoryObserver_t _lowRRDMAMemObserver;

private: // Not needed
    VirtualNetworkStack(const VirtualNetworkStack& o) = delete;
    VirtualNetworkStack(VirtualNetworkStack&& o) = delete;
    VirtualNetworkStack& operator=(const VirtualNetworkStack& o) = delete;
    VirtualNetworkStack& operator=(VirtualNetworkStack&& o) = delete;

}; // class VirtualNetworkStack

}// k2 namespace
