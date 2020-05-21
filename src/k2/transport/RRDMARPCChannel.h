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
#include <seastar/net/api.hh> // seastar's network stuff
#include <seastar/net/rdma.hh>

// k2
#include <k2/common/Common.h>
#include "RPCParser.h"
#include "TXEndpoint.h"
#include "Request.h"
#include "RPCHeader.h"
#include "BaseTypes.h"


namespace k2 {

// A RRDMA channel wraps a seastar connected_socket with an RPCParser to enable sending and receiving
// RPC messages over a RRDMA connection
// The class provides Observer interface to allow for user to observe RPC messages coming over this channel
class RRDMARPCChannel {
public: // lifecycle
    // Construct a new channel, wrapping an existing rdma connection to a client at the given address
    RRDMARPCChannel(std::unique_ptr<seastar::rdma::RDMAConnection> rconn, TXEndpoint endpoint,
                  RequestObserver_t requestObserver, FailureObserver_t failureObserver);

    // destructor
    ~RRDMARPCChannel();

    // close the channel gracefully within the given timeout.
    // That is, we stop reading data, and we try to flush out any pending writes
    // returns a future which completes when we shut down, or when timeout expires with an exception
    seastar::future<> gracefulClose(Duration timeout={});

public: // API
    // Invokes the remote rpc for the given verb with the given payload. This is an asyncronous API. No guarantees
    // are made on the delivery of the payload after the call returns.
    // The RPC message is configured with the given metadata
    void send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata meta);

    // Call this method with a callback to observe incoming RPC messages
    void registerMessageObserver(RequestObserver_t observer);

    // Call this method with a callback to observe the failure of this channel (e.g. tcp connection breakage)
    void registerFailureObserver(FailureObserver_t observer);

    // Obtain the endpoint for this channel
    TXEndpoint& getTXEndpoint();

    // This method needs to be called so that the channel can begin processing messages
    void run();

private: // fields
    // this is the RPC message parser
    RPCParser _rpcParser;

    // the observer for rpc messages
    RequestObserver_t _messageObserver;

    // the observer for connection failures
    FailureObserver_t _failureObserver;

    // the endpoint for the channel
    TXEndpoint _endpoint;

    // this holds the underlying rdma connection
    std::unique_ptr<seastar::rdma::RDMAConnection> _rconn;

    // helper method used to close the rconnection
    void _closeRconn();

    // flag to tell if the channel is closing
    bool _closingInProgress;

    // this future tells us if the close sequence has completed
    seastar::future<> _closeDoneFuture = seastar::make_ready_future();
    // this future tells us if the read loop has completed
    seastar::future<> _loopDoneFuture = seastar::make_ready_future();

    // flag to determine if we're running
    bool _running;

private: // Not needed
    RRDMARPCChannel(const RRDMARPCChannel& o) = delete;
    RRDMARPCChannel(RRDMARPCChannel&& o) = delete;
    RRDMARPCChannel& operator=(const RRDMARPCChannel& o) = delete;
    RRDMARPCChannel& operator=(RRDMARPCChannel&& o) = delete;

}; // RRDMARPCChannel
} //k2
