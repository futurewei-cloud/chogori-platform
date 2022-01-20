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

#include "RRDMARPCChannel.h"

#include <k2/config/Config.h>
#include "Log.h"

namespace k2 {

RRDMARPCChannel::RRDMARPCChannel(std::unique_ptr<seastar::rdma::RDMAConnection> rconn, TXEndpoint endpoint,
                  RequestObserver_t requestObserver, FailureObserver_t failureObserver):
    _rpcParser([]{return seastar::need_preempt();}, Config()["enable_tx_checksum"].as<bool>()),
    _endpoint(std::move(endpoint)),
    _rconn(std::move(rconn)),
    _closingInProgress(false),
    _running(false) {
    K2LOG_D(log::tx, "new channel");
    registerMessageObserver(requestObserver);
    registerFailureObserver(failureObserver);
}

RRDMARPCChannel::~RRDMARPCChannel(){
    K2LOG_D(log::tx, "dtor");
    if (!_closingInProgress) {
        K2LOG_W(log::tx, "destructor without graceful close: {}", _endpoint.url);
    }
}

void RRDMARPCChannel::send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata) {
    if (_closingInProgress) {
        K2LOG_W(log::tx, "channel is going down. ignoring send");
        return;
    }
    _rconn->send(_rpcParser.prepareForSend(verb, std::move(payload), std::move(metadata)));
}

void RRDMARPCChannel::run() {
    _running = true;
    K2LOG_D(log::tx, "Setting rdma connection")
    _rpcParser.registerMessageObserver(
        [this](Verb verb, MessageMetadata metadata, std::unique_ptr<Payload> payload) {
            this->_messageObserver(Request(verb, _endpoint, std::move(metadata), std::move(payload)));
        }
    );
    _rpcParser.registerParserFailureObserver(
        [this](std::exception_ptr exc) {
            K2LOG_W_EXC(log::tx, exc, "Received parser exception");
            this->_failureObserver(this->_endpoint, exc);
        }
    );

    // setup read loop
    _loopDoneFuture = seastar::do_until(
        [this] { return _rconn->closed(); }, // end condition for loop
        [this] () mutable { // body of loop
            if (_rpcParser.canDispatch()) {
                K2LOG_D(log::tx, "RPC parser can dispatch more messages as-is. not reading from socket this round");
                _rpcParser.dispatchSome();
                return seastar::make_ready_future();
            }
            return _rconn->recv().
                then([this](Binary&& packet) mutable {
                    if (packet.empty()) {
                        K2LOG_D(log::tx, "remote end closed connection");
                        return; // just say we're done so the loop can evaluate the end condition
                    }
                    _rpcParser.feed(std::move(packet));
                    // process some messages from the packet
                    _rpcParser.dispatchSome();
                }).
                handle_exception([] (auto exc) {
                    // let the loop go and check the condition above. Upon exception, the connection should be closed
                    K2LOG_W_EXC(log::tx, exc, "Exception while reading connection");
                    return seastar::make_ready_future();
                });
        }
    ).finally([this]() {
        // close the connection if it wasn't closed already
        _closeRconn();
    });
}

void RRDMARPCChannel::registerMessageObserver(RequestObserver_t observer) {
    K2LOG_D(log::tx, "register msg observer");
    if (observer == nullptr) {
        K2LOG_D(log::tx, "Setting default message observer");
        _messageObserver = [this](Request&& request) {
            if (!this->_closingInProgress) {
                K2LOG_W(log::tx, "Message: verb={} ignored since there is no message observer registered...", request.verb);
            }
        };
    }
    else {
        _messageObserver = observer;
    }
}

void RRDMARPCChannel::registerFailureObserver(FailureObserver_t observer) {
    K2LOG_D(log::tx, "register failure observer");
    if (observer == nullptr) {
        K2LOG_D(log::tx, "Setting default failure observer");
        _failureObserver = [this](TXEndpoint&, std::exception_ptr exc) {
            if (!this->_closingInProgress) {
                K2LOG_W_EXC(log::tx, exc, "Ignoring failure since there is no failure observer registered...");
            }
        };
    }
    else {
        _failureObserver = observer;
    }
}

seastar::future<> RRDMARPCChannel::gracefulClose(Duration timeout) {
    // TODO, setup a timer for shutting down
    (void) timeout;
    K2LOG_D(log::tx, "graceful close");
    // close the connection if it wasn't closed already
    _closeRconn();

    return seastar::when_all_succeed(std::move(_closeDoneFuture), std::move(_loopDoneFuture)).discard_result();
}

void RRDMARPCChannel::_closeRconn() {
    K2LOG_D(log::tx, "Closing socket: ipr={}", _closingInProgress);
    if (!_closingInProgress) {
        _closingInProgress = true;
        _closeDoneFuture = _rconn->close();
        // signal the Protocol that this channel has closed
        _failureObserver(_endpoint, nullptr);
    }
}

TXEndpoint& RRDMARPCChannel::getTXEndpoint() { return _endpoint; }

} // k2
