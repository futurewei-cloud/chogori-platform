//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCChannel.h"

// third-party
#include <seastar/net/inet_address.hh>

namespace k2 {

TCPRPCChannel::TCPRPCChannel(seastar::future<seastar::connected_socket> futureSocket, TXEndpoint endpoint,
                  RequestObserver_t requestObserver, FailureObserver_t failureObserver):
    _rpcParser([]{return seastar::need_preempt();}),
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
    _futureSocket.then([chan=weak_from_this()](seastar::connected_socket fd) {
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
    assert(payload->getSize() >= txconstants::MAX_HEADER_SIZE);
    auto dataSize = payload->getSize() - txconstants::MAX_HEADER_SIZE;
    if (dataSize > 0) {
        metadata.setPayloadSize(dataSize);
    }
    K2DEBUG("send: verb=" << verb << ", payloadSize="<< dataSize);

    if (_closingInProgress) {
        K2WARN("channel is going down. ignoring send");
        return;
    }
    if (!_fdIsSet) {
        // we don't have a connected socket yet. Queue up the request
        K2DEBUG("send: not connected yet. Buffering the write, have buffered already " << _pendingWrites.size());
        _pendingWrites.emplace_back(_BufferedWrite{verb, std::move(payload), std::move(metadata)});
        return;
    }
    // disassemble the payload so that we can write the header in the first binary
    auto&& buffers = payload->release();
    // write the header into the headroom of the first binary and remember to send the extra bytes
    dataSize += RPCParser::serializeHeader(buffers[0], verb, std::move(metadata));
    // write out the header and data
    K2DEBUG("writing message: verb=" << verb << ", messageSize="<< dataSize);

    _sendFuture = _sendFuture->then([buffers = std::move(buffers), dataSize, chan=weak_from_this()]() mutable {
        if (chan) {
            return seastar::do_for_each(buffers, [dataSize, chan=chan->weak_from_this()](auto& buf) mutable {
                if (chan) {
                    if (buf.size() > dataSize) {
                        buf.trim(dataSize);
                    }
                    dataSize -= buf.size();
                    return chan->_out.write(std::move(k2::toCharTempBuffer(buf)));
                }
                return seastar::make_ready_future<>();
            });
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
    seastar::do_until(
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
                then([chan=chan->weak_from_this()](seastar::temporary_buffer<char> packet) {
                    if (chan) {
                        if (packet.empty()) {
                            K2DEBUG("remote end closed connection");
                            return; // just say we're done so the loop can evaluate the end condition
                        }
                        K2DEBUG("Read "<< packet.size());
                        chan->_rpcParser.feed(std::move(k2::toBinary(packet)));
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
    for(auto& write: _pendingWrites) {
        send(write.verb, std::move(write.payload), std::move(write.meta));
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
