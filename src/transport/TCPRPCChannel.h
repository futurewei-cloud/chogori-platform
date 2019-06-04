//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

// third-party
#include <seastar/net/api.hh> // seastar's network stuff
#include <seastar/core/weak_ptr.hh> // weak ptr
#include <seastar/util/std-compat.hh>

// k2
#include "common/Common.h"
#include "RPCParser.h"
#include "TXEndpoint.h"
#include "Request.h"
#include "RPCHeader.h"
#include "BaseTypes.h"


namespace k2 {

// A TCP channel wraps a seastar connected_socket with an RPCParser to enable sending and receiving
// RPC messages over a TCP connection
// The class provides Observer interface to allow for user to observe RPC messages coming over this channel
class TCPRPCChannel: public seastar::weakly_referencable<TCPRPCChannel> {

public: // lifecycle
    // Construct a new channel, wrapping a future connected socket to a client at the given address
    TCPRPCChannel(seastar::future<seastar::connected_socket> futureSocket, TXEndpoint endpoint,
                  RequestObserver_t requestObserver, FailureObserver_t failureObserver);

    // destructor
    ~TCPRPCChannel();

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
    TXEndpoint& getTXEndpoint() { return _endpoint;}

    // This method needs to be called so that the channel can begin processing messages
    void run();

private: // methods
    // we call this method when we successfully connect to a remote end.
    // While we're connecting, any writes are queued in the channel and now we have to flush them
    void _processQueuedWrites();

    // helper method to setup an incoming connected socket
    void _setConnectedSocket(seastar::connected_socket sock);

private: // fields
    // this is the RPC message parser
    RPCParser _rpcParser;

    // the observer for rpc messages
    RequestObserver_t _messageObserver;

    // the observer for connection failures
    FailureObserver_t _failureObserver;

    // the endpoint for the channel
    TXEndpoint _endpoint;

    // this holds the underlying socket
    seastar::connected_socket _fd;

    // flag we use to tell if the _fd is set
    bool _fdIsSet;

    // flag to tell if the channel is closing
    bool _closingInProgress;

    seastar::future<> _closerFuture = seastar::make_ready_future<>();

    // the input stream from our socket
    seastar::input_stream<char> _in;

    // the output stream from our socket
    seastar::output_stream<char> _out;

    // a place to store pending writes while we're connecting
    struct _BufferedWrite {
        Verb verb;
        std::unique_ptr<Payload> payload;
        MessageMetadata meta;
    };
    std::vector<_BufferedWrite> _pendingWrites;

    // flag to determine if we're running
    bool _running;

    // used during intialization to process incoming connected_socket
    seastar::future<seastar::connected_socket> _futureSocket;

    // used to properly chain sends
    seastar::compat::optional<seastar::future<>> _sendFuture;

private: // Not needed
    TCPRPCChannel(const TCPRPCChannel& o) = delete;
    TCPRPCChannel(TCPRPCChannel&& o) = delete;
    TCPRPCChannel& operator=(const TCPRPCChannel& o) = delete;
    TCPRPCChannel& operator=(TCPRPCChannel&& o) = delete;

}; // TCPRPCChannel
} //k2
