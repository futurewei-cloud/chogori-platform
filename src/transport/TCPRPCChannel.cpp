//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCChannel.h"

// third-party
#include <seastar/net/inet_address.hh>

// k2
#include "common/Log.h"

#define CDEBUG(msg) K2DEBUG("{conn="<< (void*)this << ", addr=" << this->_endpoint.GetURL() << "} " << msg)
#define CHDEBUG(msg) { \
    if (chan) { \
        K2DEBUG("{conn="<< (void*)(chan.get()) << ", addr=" << chan->_endpoint.GetURL() << "} " << msg); \
    } \
    else { \
        K2DEBUG("{channel has been destroyed} " << msg); \
    } \
}

#define CWARN(msg) K2WARN("{conn="<< (void*)this << ", addr=" << this->_endpoint.GetURL() << "} " << msg)

namespace k2 {

TCPRPCChannel::TCPRPCChannel(seastar::connected_socket fd, TXEndpoint endpoint):
    _rpcParser([]{return seastar::need_preempt();}),
    _endpoint(std::move(endpoint)),
    _fdIsSet(false),
    _closingInProgress(false) {
    CDEBUG("new channel");
    RegisterMessageObserver(nullptr);
    RegisterFailureObserver(nullptr);
    _setConnectedSocket(std::move(fd));
}

TCPRPCChannel::TCPRPCChannel(seastar::future<seastar::connected_socket> futureSocket, TXEndpoint endpoint):
    _rpcParser([]{return seastar::need_preempt();}),
    _endpoint(std::move(endpoint)),
    _fdIsSet(false),
    _closingInProgress(false) {
    CDEBUG("new future channel");
    RegisterMessageObserver(nullptr);
    RegisterFailureObserver(nullptr);
    futureSocket.then([chan=weak_from_this()](seastar::connected_socket fd) {
        if (chan) {
            K2DEBUG("future channel for " << chan->_endpoint.GetURL() << " connected successfully");
            if (chan->_closingInProgress) {
                K2WARN("channel is going down. ignoring completed connect for " << chan->_endpoint.GetURL());
                return;
            }
            chan->_setConnectedSocket(std::move(fd));
        }
    }).handle_exception([chan=weak_from_this()](auto exc) {
        if (chan) {
            K2WARN("future channel for " << chan->_endpoint.GetURL() << " failed connecting");
            chan->_failureObserver(chan->_endpoint, exc);
        }
    });
}

TCPRPCChannel::~TCPRPCChannel(){
    CDEBUG("dtor");
    if (!_closingInProgress) {
        K2WARN("destructor without graceful close");
    }
}

void TCPRPCChannel::Send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata, bool flush) {
    CDEBUG("send: verb=" << verb << ", payloadSize="<< payload->getSize() << ", flush=" << flush);
    if (_closingInProgress) {
        K2WARN("channel is going down. ignoring send");
        return;
    }
    if (!_fdIsSet) {
        // we don't have a connected socket yet. Queue up the request
        CDEBUG("send: not connected yet. Buffering the write, have buffered already " << _pendingWrites.size());
        _pendingWrites.emplace_back(_BufferedWrite{verb, std::move(payload), std::move(metadata)});
        return;
    }
    // Messages are written in two parts: the header and the payload.
    // ask the serializer to write out its header to a binary
    CDEBUG("writing header: verb=" << verb << ", payloadSize="<< payload->getSize() << ", flush=" << flush);
    auto header = _endpoint.NewBinary();
    RPCParser::SerializeHeader(header, verb, std::move(metadata));

    // cast to seastar-compatible type
    _out.write(std::move(k2::toCharTempBuffer(header)));

    // payload is optional
    if (payload->getSize() > 0) {
        CDEBUG("writing payload: verb=" << verb << ", payloadSize="<< payload->getSize() << ", flush=" << flush);
        // we have some payload to write
        auto bytesToWrite = payload->getSize();
        CDEBUG("Payload size is now: " << bytesToWrite);

        for (auto& buf: payload->release()) {
            if (buf.size() > bytesToWrite) {
                buf.trim(bytesToWrite);
            }
            _out.write(std::move(k2::toCharTempBuffer(buf)));
            bytesToWrite -= buf.size();
        }
    }
    // RIP payload...
    if (flush) {
        CDEBUG("write with flush...");
        _out.flush();
    }
}

void TCPRPCChannel::_setConnectedSocket(seastar::connected_socket sock) {
    CDEBUG("Setting connected socket")
    assert(!_fdIsSet);
    _fdIsSet = true;
    _fd = std::move(sock);
    _in = _fd.input();
    _out = _fd.output();
    _rpcParser.RegisterMessageObserver(
        [this](Verb verb, MessageMetadata metadata, std::unique_ptr<Payload> payload) {
            CDEBUG("Received message with verb: " << verb);
            Request req(verb, _endpoint, std::move(metadata), std::move(payload));
            this->_messageObserver(req);
        }
    );
    _rpcParser.RegisterParserFailureObserver(
        [this](std::exception_ptr exc) {
            CDEBUG("Received parser exception " << exc);
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
            if (chan->_rpcParser.CanDispatch()) {
                CHDEBUG("RPC parser can dispatch more messages as-is. not reading from socket this round");
                chan->_rpcParser.DispatchSome();
                return seastar::make_ready_future<>();
            }
            return chan->_in.read().
                then([chan=chan->weak_from_this()](seastar::temporary_buffer<char> packet) {
                    if (chan) {
                        if (packet.empty()) {
                            CHDEBUG("remote end closed connection in conn for " << chan->_endpoint.GetURL());
                            return; // just say we're done so the loop can evaluate the end condition
                        }
                        CHDEBUG("Read "<< packet.size());
                        chan->_rpcParser.Feed(std::move(k2::toBinary(packet)));
                        // process some messages from the packet
                        chan->_rpcParser.DispatchSome();
                    }
                }).
                handle_exception([chan=chan->weak_from_this()] (auto) {
                    return seastar::make_ready_future<>();
            });
        }
    )
    .finally([chan=weak_from_this()] {
        CHDEBUG("do_until is done");
        if (chan) {
            chan->_closerFuture = chan->GracefulClose();
        }
        return seastar::make_ready_future<>();
    }); // finally
}

void TCPRPCChannel::RegisterMessageObserver(MessageObserver_t observer) {
    CDEBUG("register msg observer");
    if (observer == nullptr) {
        CDEBUG("Setting default message observer");
        _messageObserver = [this](Request& request) {
            if (!this->_closingInProgress) {
                CWARN("Message: " << request.verb
                << " ignored since there is no message observer registered...");
            }
        };
    }
    else {
        _messageObserver = observer;
    }
}

void TCPRPCChannel::RegisterFailureObserver(FailureObserver_t observer) {
    CDEBUG("register failure observer");
    if (observer == nullptr) {
        CDEBUG("Setting default failure observer");
        _failureObserver = [this](TXEndpoint& endpoint, std::exception_ptr) {
            if (!this->_closingInProgress) {
                CWARN("Ignoring failure, from " << endpoint.GetURL()
                    << ", since there is no failure observer registered...");
            }
        };
    }
    else {
        _failureObserver = observer;
    }
}

void TCPRPCChannel::_processQueuedWrites() {
    CDEBUG("pending writes: " << _pendingWrites.size());
    for(auto& write: _pendingWrites) {
        Send(write.verb, std::move(write.payload), std::move(write.meta));
    }
    _pendingWrites.resize(0); // reclaim any memory used by the vector
}

seastar::future<> TCPRPCChannel::GracefulClose(Duration timeout) {
    // TODO, setup a timer for shutting down
    (void) timeout;
    CDEBUG("graceful close")
    if (_closingInProgress) {
        CDEBUG("already closing...no-op");
        return std::move(_closerFuture);
    }
    _closingInProgress = true;
    if (!_fdIsSet) {
        CDEBUG("we aren't connected anyway so nothing to do...")
        return seastar::make_ready_future<>();
    }

    // shutdown protocol
    // 1. close input sink (to break any potential read promises)
    return _in.close()
    .then_wrapped([chan=weak_from_this()](auto&& fut) {
        CHDEBUG("input close completed");
        // ignore any flushing issues
        fut.ignore_ready_future();
        // 2. tell poller to stop polling for input
        if (chan) {
            // this may throw and we need to make sure we close below
            try {chan->_fd.shutdown_input();}catch(...){}
            // 3. flush & close the output close() flushes before closing
            return chan->_out.close();
        }
        else {
            K2WARN("graceful sequence failed: object got deleted at step 2");
            return seastar::make_ready_future<>();
        }
    })
    .then_wrapped([chan=weak_from_this()](auto&& fut){
        CHDEBUG("output close completed");
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
