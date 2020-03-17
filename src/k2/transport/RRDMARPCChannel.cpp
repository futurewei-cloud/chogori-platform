//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "RRDMARPCChannel.h"

#include <k2/config/Config.h>

namespace k2 {

RRDMARPCChannel::RRDMARPCChannel(std::unique_ptr<seastar::rdma::RDMAConnection> rconn, TXEndpoint endpoint,
                  RequestObserver_t requestObserver, FailureObserver_t failureObserver):
    _rpcParser([]{return seastar::need_preempt();}, Config()["enable_tx_checksum"].as<bool>()),
    _endpoint(std::move(endpoint)),
    _rconn(std::move(rconn)),
    _closingInProgress(false),
    _running(false) {
    K2DEBUG("new channel");
    registerMessageObserver(requestObserver);
    registerFailureObserver(failureObserver);
}

RRDMARPCChannel::~RRDMARPCChannel(){
    K2DEBUG("dtor");
    if (!_closingInProgress) {
        K2WARN("destructor without graceful close");
    }
}

void RRDMARPCChannel::send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata) {
    assert(_running);
    if (_closingInProgress) {
        K2WARN("channel is going down. ignoring send");
        return;
    }
    _rconn->send(_rpcParser.prepareForSend(verb, std::move(payload), std::move(metadata)));
}

void RRDMARPCChannel::run() {
    assert(!_running);
    _running = true;
    K2DEBUG("Setting rdma connection")
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

    // setup read loop
    _loopDoneFuture = seastar::do_until(
        [this] { return _rconn->closed(); }, // end condition for loop
        [this] () mutable { // body of loop
            if (_rpcParser.canDispatch()) {
                K2DEBUG("RPC parser can dispatch more messages as-is. not reading from socket this round");
                _rpcParser.dispatchSome();
                return seastar::make_ready_future();
            }
            return _rconn->recv().
                then([this](Binary&& packet) mutable {
                    if (packet.empty()) {
                        K2DEBUG("remote end closed connection");
                        return; // just say we're done so the loop can evaluate the end condition
                    }
                    K2DEBUG("Read "<< packet.size());
                    _rpcParser.feed(std::move(packet));
                    // process some messages from the packet
                    _rpcParser.dispatchSome();
                }).
                handle_exception([] (auto exc) {
                    // let the loop go and check the condition above. Upon exception, the connection should be closed
                    K2WARN_EXC("Exception while reading connection", exc);
                    return seastar::make_ready_future();
                });
        }
    ).finally([this]() {
        // close the connection if it wasn't closed already
        _closeRconn();
    });
}

void RRDMARPCChannel::registerMessageObserver(RequestObserver_t observer) {
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

void RRDMARPCChannel::registerFailureObserver(FailureObserver_t observer) {
    K2DEBUG("register failure observer");
    if (observer == nullptr) {
        K2DEBUG("Setting default failure observer");
        _failureObserver = [this](TXEndpoint&, std::exception_ptr) {
            if (!this->_closingInProgress) {
                K2WARN("Ignoring failure since there is no failure observer registered...");
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
    K2DEBUG("graceful close");
    // close the connection if it wasn't closed already
    _closeRconn();

    return seastar::when_all(std::move(_closeDoneFuture), std::move(_loopDoneFuture)).discard_result();
}

void RRDMARPCChannel::_closeRconn() {
    K2DEBUG("Closing socket: " << _closingInProgress);
    if (!_closingInProgress) {
        _closingInProgress = true;
        _closeDoneFuture = _rconn->close();
    }
}

TXEndpoint& RRDMARPCChannel::getTXEndpoint() { return _endpoint; }

} // k2
