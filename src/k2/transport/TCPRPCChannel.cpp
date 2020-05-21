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
    _loopDoneFuture = _futureSocket.then([this](seastar::connected_socket&& fd) {
        K2DEBUG("future channel connected successfully");
        if (_closingInProgress) {
            K2WARN("channel is going down. ignoring completed connect");
            return seastar::make_ready_future();
        }
        return _setConnectedSocket(std::move(fd));
    }).handle_exception([this](auto exc) {
        K2WARN("future channel failed connecting");
        _failureObserver(_endpoint, exc);
        return seastar::make_ready_future();
    });
}

void TCPRPCChannel::send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata) {
    assert(_running);
    K2DEBUG("send: verb=" << int(verb));
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
    _sendFuture = _sendFuture->then([packet = std::move(packet), this]() mutable {
        return _out.write(std::move(packet));
    }).then([this]() {
        return _out.flush();
    });
}

seastar::future<> TCPRPCChannel::_setConnectedSocket(seastar::connected_socket sock) {
    K2DEBUG("Setting connected socket")
    assert(!_fdIsSet);
    _fdIsSet = true;
    _fd = std::move(sock);
    _in = _fd.input();
    _out = _fd.output();
    _rpcParser.registerMessageObserver(
        [this](Verb verb, MessageMetadata metadata, std::unique_ptr<Payload> payload) {
            K2DEBUG("Received message with verb: " << int(verb));
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
    return seastar::do_until(
        [this] { return _in.eof(); }, // end condition for loop
        [this] { // body of loop
            if (_rpcParser.canDispatch()) {
                K2DEBUG("RPC parser can dispatch more messages as-is. not reading from socket this round");
                _rpcParser.dispatchSome();
                return seastar::make_ready_future<>();
            }
            return _in.read().
                then([this](Binary&& packet) {
                    if (packet.empty()) {
                        K2DEBUG("remote end closed connection");
                        return; // just say we're done so the loop can evaluate the end condition
                    }
                    K2DEBUG("Read "<< packet.size());
                    _rpcParser.feed(std::move(packet));
                    // process some messages from the packet
                    _rpcParser.dispatchSome();
                }).
                handle_exception([this] (auto) {
                    K2DEBUG("ignoring exception");
                    // ignore the incoming exception as the input stream.eof() should indicate it's closed
                    return seastar::make_ready_future<>();
                });
        }
    ).finally([this]() {
        // close the socket
        K2DEBUG("loop ended");
        _closeSocket();
    });
}

void TCPRPCChannel::registerMessageObserver(RequestObserver_t observer) {
    K2DEBUG("register msg observer");
    if (observer == nullptr) {
        K2DEBUG("Setting default message observer");
        _messageObserver = [this](Request&& request) {
            if (!_closingInProgress) {
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
            if (!_closingInProgress) {
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
    _closeSocket();

    return seastar::when_all_succeed(std::move(_closeDoneFuture), std::move(_loopDoneFuture)).discard_result();
}

void TCPRPCChannel::_closeSocket() {
    K2DEBUG("Closing socket: ipr=" << _closingInProgress <<", fdIsSet=" << _fdIsSet);
    if (!_closingInProgress) {
        _closingInProgress = true;

        // shutdown protocol
        // 1. close input sink (to break any potential read promises)
        _closeDoneFuture = _sendFuture->
        then([this]() {
            K2DEBUG("closing input");
            if (_fdIsSet) {
                return _in.close();
            }
            return seastar::make_ready_future();
        })
        .then_wrapped([this](auto&& fut) {
            K2DEBUG("input close completed");
            if (_fdIsSet) {
                // ignore any flushing issues
                fut.ignore_ready_future();
                // 2. tell poller to stop polling for input
                // this may throw and we need to make sure we close below
                try {_fd.shutdown_input();}catch(...){}
                // 3. flush & close the output close() flushes before closing
                return _out.close();
            }
            return seastar::make_ready_future();
        })
        .then_wrapped([this](auto&& fut){
            K2DEBUG("output close completed");
            if (_fdIsSet) {
                // ignore any closing issues
                fut.ignore_ready_future();
                // 5. tell poller we're done sending data. it may throw but we don't care
                try {
                    _fd.shutdown_output();
                } catch (...) {
                }
                // 6. this always succeeds!
                _failureObserver(_endpoint, nullptr);
            }
            return seastar::make_ready_future<>();
        });
    }
}

TXEndpoint& TCPRPCChannel::getTXEndpoint() { return _endpoint;}

} // k2
