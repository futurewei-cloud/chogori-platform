//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCChannel.h"

#include <k2/config/Config.h>

// third-party
#include <seastar/net/inet_address.hh>

namespace k2 {

TCPRPCChannel::TCPRPCChannel(seastar::future<seastar::connected_socket> futureSocket, TXEndpoint endpoint,
                  RequestObserver_t requestObserver, FailureObserver_t failureObserver):
    _rpcParser([]{return seastar::need_preempt();}, Config()["enable_tx_checksum"].as<bool>()),
    _endpoint(std::move(endpoint)),
    _fdIsSet(false),
    _closingInProgress(false),
    _running(false),
    _futureSocket(std::move(futureSocket)),
    _sendFuture(seastar::make_ready_future<>()){
    K2DEBUG("new future channel");
    registerMessageObserver(requestObserver);
    registerFailureObserver(failureObserver);
}

TCPRPCChannel::~TCPRPCChannel(){
    K2DEBUG("dtor");
    if (!_closingInProgress) {
        K2WARN("destructor without graceful close");
    }
}

void TCPRPCChannel::run() {
    assert(!_running);
    _running = true;
    (void) _futureSocket.then([chan=weak_from_this()](seastar::connected_socket fd) {
        if (chan) {
            K2DEBUG("future channel connected successfully");
            if (chan->_closingInProgress) {
                K2WARN("channel is going down. ignoring completed connect");
                return;
            }
            chan->_setConnectedSocket(std::move(fd));
        }
    }).handle_exception([chan=weak_from_this()](auto exc) {
        if (chan) {
            K2WARN("future channel failed connecting");
            chan->_failureObserver(chan->_endpoint, exc);
        }
    });
}

void TCPRPCChannel::send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata) {
    assert(_running);
    K2DEBUG("send: verb=" << verb);
    if (_closingInProgress) {
        K2WARN("channel is going down. ignoring send");
        return;
    }
    seastar::net::packet packet;
    for (auto& buf : _rpcParser.prepareForSend(verb, std::move(payload), std::move(metadata))) {
        packet = seastar::net::packet(std::move(packet), std::move(buf));
    }
    if (!_fdIsSet) {
        // we don't have a connected socket yet. Queue up the request
        K2DEBUG("send: not connected yet. Buffering the write, have buffered already " << _pendingWrites.size());
        _pendingWrites.push_back(std::move(packet));
        return;
    }
    _sendPacket(std::move(packet));
}

void TCPRPCChannel::_sendPacket(seastar::net::packet&& packet) {
    _sendFuture = _sendFuture->then([packet = std::move(packet), chan=weak_from_this()]() mutable {
        if (chan) {
            return chan->_out.write(std::move(packet));
        }
        return seastar::make_ready_future<>();
    }).then([chan=weak_from_this()]() {
        if (chan) {
            return chan->_out.flush();
        }
        return seastar::make_ready_future<>();
    });
}

void TCPRPCChannel::_setConnectedSocket(seastar::connected_socket sock) {
    K2DEBUG("Setting connected socket")
    assert(!_fdIsSet);
    _fdIsSet = true;
    _fd = std::move(sock);
    _in = _fd.input();
    _out = _fd.output();
    _rpcParser.registerMessageObserver(
        [this](Verb verb, MessageMetadata metadata, std::unique_ptr<Payload> payload) {
            K2DEBUG("Received message with verb: " << verb);
            this->_messageObserver(Request(verb, _endpoint, std::move(metadata), std::move(payload)));
        }
    );
    _rpcParser.registerParserFailureObserver(
        [this](std::exception_ptr exc) {
            K2DEBUG("Received parser exception");
            this->_failureObserver(this->_endpoint, exc);
        }
    );
    _processQueuedWrites();

    // setup read loop
    (void) seastar::do_until(
        [chan=weak_from_this()] { return !chan || chan->_in.eof(); }, // end condition for loop
        [chan=weak_from_this()] { // body of loop
            if (!chan) {
                return seastar::make_ready_future<>();
            }
            if (chan->_rpcParser.canDispatch()) {
                K2DEBUG("RPC parser can dispatch more messages as-is. not reading from socket this round");
                chan->_rpcParser.dispatchSome();
                return seastar::make_ready_future<>();
            }
            return chan->_in.read().
                then([chan=chan->weak_from_this()](Binary&& packet) {
                                       if (chan) {
                                           if (packet.empty()) {
                                               K2DEBUG("remote end closed connection");
                                               return; // just say we're done so the loop can evaluate the end condition
                                           }
                                           K2DEBUG("Read "<< packet.size());
                                           chan->_rpcParser.feed(std::move(packet));
                                           // process some messages from the packet
                                           chan->_rpcParser.dispatchSome();
                                       }
                }).
                handle_exception([chan=chan->weak_from_this()] (auto) {
                    return seastar::make_ready_future<>();
                });
        }
    )
    .finally([chan=weak_from_this()] {
        K2DEBUG("do_until is done");
        if (chan) {
            chan->_closerFuture = chan->gracefulClose();
        }
        return seastar::make_ready_future<>();
    }); // finally
}

void TCPRPCChannel::registerMessageObserver(RequestObserver_t observer) {
    K2DEBUG("register msg observer");
    if (observer == nullptr) {
        K2DEBUG("Setting default message observer");
        _messageObserver = [this](Request&& request) {
            if (!this->_closingInProgress) {
                K2WARN("Message: " << request.verb
                << " ignored since there is no message observer registered...");
            }
        };
    }
    else {
        _messageObserver = observer;
    }
}

void TCPRPCChannel::registerFailureObserver(FailureObserver_t observer) {
    K2DEBUG("register failure observer");
    if (observer == nullptr) {
        K2DEBUG("Setting default failure observer");
        _failureObserver = [this](TXEndpoint&, std::exception_ptr) {
            if (!this->_closingInProgress) {
                K2WARN("Ignoring failure, since there is no failure observer registered...");
            }
        };
    }
    else {
        _failureObserver = observer;
    }
}

void TCPRPCChannel::_processQueuedWrites() {
    K2DEBUG("pending writes: " << _pendingWrites.size());
    for(auto& packet: _pendingWrites) {
        _sendPacket(std::move(packet));
    }
    _pendingWrites.resize(0); // reclaim any memory used by the vector
}

seastar::future<> TCPRPCChannel::gracefulClose(Duration timeout) {
    // TODO, setup a timer for shutting down
    (void) timeout;
    K2DEBUG("graceful close")
    if (_closingInProgress) {
        K2DEBUG("already closing...no-op");
        return std::move(_closerFuture);
    }
    _closingInProgress = true;
    if (!_fdIsSet) {
        K2DEBUG("we aren't connected anyway so nothing to do...")
        return seastar::make_ready_future<>();
    }

    // shutdown protocol
    // 1. close input sink (to break any potential read promises)
    return _sendFuture->then([chan=weak_from_this()]() {
        if (chan) {
            return chan->_in.close();
        }
        return seastar::make_ready_future<>();
    })
    .then_wrapped([chan=weak_from_this()](auto&& fut) {
        K2DEBUG("input close completed");
        // ignore any flushing issues
        fut.ignore_ready_future();
        // 2. tell poller to stop polling for input
        if (chan) {
            // this may throw and we need to make sure we close below
            try {chan->_fd.shutdown_input();}catch(...){}
            // 3. flush & close the output close() flushes before closing
            return chan->_out.close();
        }
        K2WARN("graceful sequence failed: object got deleted at step 2");
        return seastar::make_ready_future<>();
    })
    .then_wrapped([chan=weak_from_this()](auto&& fut){
        K2DEBUG("output close completed");
        // ignore any closing issues
        fut.ignore_ready_future();
        if (chan) {
            // 5. tell poller we're done sending data. it may throw but we don't care
            try {chan->_fd.shutdown_output();}catch(...){}
            // 6. this always succeeds!
            chan->_failureObserver(chan->_endpoint, nullptr);

        }
        else {
            K2WARN("graceful sequence failed: object got deleted at step 5");
        }
        return seastar::make_ready_future<>();
    });
}

} // k2
