//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "RRDMARPCChannel.h"

namespace k2 {

RRDMARPCChannel::RRDMARPCChannel(std::unique_ptr<seastar::rdma::RDMAConnection> rconn, TXEndpoint endpoint,
                  RequestObserver_t requestObserver, FailureObserver_t failureObserver):
    _rpcParser([]{return seastar::need_preempt();}),
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

    // disassemble the payload so that we can write the header in the first binary
    auto&& buffers = payload->release();
    // write the header into the headroom of the first binary and remember to send the extra bytes
    dataSize += RPCParser::serializeHeader(buffers[0], verb, std::move(metadata));
    // write out the header and data
    K2DEBUG("writing message: verb=" << verb << ", messageSize="<< dataSize);
    size_t bufIdx = 0;
    while(bufIdx < buffers.size() && dataSize > 0) {
        auto& buf = buffers[bufIdx];
        if (buf.size() > dataSize) {
            buf.trim(dataSize);
        }
        dataSize -= buf.size();
        ++bufIdx;
    }
    buffers.resize(std::min(buffers.size(), bufIdx)); // remove any unneeded binaries
    _rconn->send(std::move(buffers));
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
    (void) seastar::do_until(
        [chan=weak_from_this()] { return !chan || chan->_rconn->closed(); }, // end condition for loop
        [chan=weak_from_this()] { // body of loop
            if (!chan) {
                return seastar::make_ready_future<>();
            }
            if (chan->_rpcParser.canDispatch()) {
                K2DEBUG("RPC parser can dispatch more messages as-is. not reading from socket this round");
                chan->_rpcParser.dispatchSome();
                return seastar::make_ready_future<>();
            }
            return chan->_rconn->recv().
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
    K2DEBUG("graceful close")
    if (_closingInProgress) {
        K2DEBUG("already closing...no-op");
        return std::move(_closerFuture);
    }
    _closingInProgress = true;

    return _rconn->close();
}

} // k2
