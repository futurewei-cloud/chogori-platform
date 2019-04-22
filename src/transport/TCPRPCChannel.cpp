//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCChannel.h"

// third-party
#include <seastar/net/inet_address.hh>

// k2tx
#include "Log.h"

#define CDEBUG(msg) K2DEBUG("{conn="<< (void*)this << ", addr=" << this->_endpoint.GetURL() << "} " << msg)
#define CHDEBUG(msg) K2DEBUG("{conn="<< (void*)(chan.get()) << ", addr=" << chan->_endpoint.GetURL() << "} " << msg)
#define CWARN(msg) K2WARN("{conn="<< (void*)this << ", addr=" << this->_endpoint.GetURL() << "} " << msg)

namespace k2tx {

TCPRPCChannel::TCPRPCChannel(seastar::connected_socket fd, Endpoint endpoint):
    _endpoint(std::move(endpoint)),
    _fdIsSet(false) {
    CDEBUG("new channel");
    RegisterMessageObserver(nullptr);
    RegisterFailureObserver(nullptr);
    _setConnectedSocket(std::move(fd));
}

TCPRPCChannel::TCPRPCChannel(seastar::future<seastar::connected_socket> futureSocket, Endpoint endpoint):
    _endpoint(std::move(endpoint)) {
    CDEBUG("new future channel");
    RegisterMessageObserver(nullptr);
    RegisterFailureObserver(nullptr);
    futureSocket.then([chan=weak_from_this()](seastar::connected_socket fd) {
        if (chan) {
            K2DEBUG("future channel for " << chan->_endpoint.GetURL() << " connected successfully");
            chan->_setConnectedSocket(std::move(fd));
        }
    }).handle_exception([chan=weak_from_this()](auto exc) {
        if (chan) {
            K2WARN("future channel for " << chan->_endpoint.GetURL() << " failed connecting: " << exc);
            chan->_failureObserver(chan->_endpoint, exc);
        }
    });
}

TCPRPCChannel::~TCPRPCChannel(){
    CDEBUG("dtor");
}

void TCPRPCChannel::Send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata, bool flush) {
    CDEBUG("send: verb=" << verb << ", payloadSize="<< payload->Size() << ", flush=" << flush);
    if (!_fdIsSet) {
        // we don't have a connected socket yet. Queue up the request
        CDEBUG("send: not connected yet. Buffering the write...");
        _pendingWrites.emplace_back(_BufferedWrite{verb, std::move(payload), std::move(metadata)});
        return;
    }
    // Messages are written in two parts: the header and the payload.
    // ask the serializer to write out its header to a fragment
    CDEBUG("writing header: verb=" << verb << ", payloadSize="<< payload->Size() << ", flush=" << flush);
    _out.write(RPCParser::SerializeHeader(_endpoint.NewFragment(), verb, std::move(metadata)));

    // payload is optional
    if (payload->Size() > 0) {
        CDEBUG("writing payload: verb=" << verb << ", payloadSize="<< payload->Size() << ", flush=" << flush);
        // we have some payload to write
        auto& fragments = payload->Fragments();

        // write all but last fragment as-is
        for (size_t i = 0; i < fragments.size() - 1; ++i) {
            CDEBUG("write intermediate fragment of size=" << fragments[i].size());
            _out.write(std::move(fragments[i]));
        }
        if (payload->LastFragmentSize() > 0) {
            // we only write LastFragmentSize bytes from the last fragment
            fragments.back().trim(payload->LastFragmentSize());
            CDEBUG("write last fragment of size=" << fragments.back().size());
            _out.write(std::move(fragments.back()));
        }
    }
    // at this point payload.Fragments() still has the same elements but they are empty. However we're done with payload,
    // so no need to do anything with that vector
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
            this->_messageObserver(std::move(req));
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
            if (chan) {
                if (chan->_rpcParser.CanDispatch()) {
                    CHDEBUG("RPC parser can dispatch more messages as-is. not reading from socket this round");
                    chan->_rpcParser.DispatchSome();
                    return seastar::make_ready_future<>();
                }
                return chan->_in.read().then(
                    [chan=chan->weak_from_this()](seastar::temporary_buffer<char> packet) {
                        if (chan) {
                            if (packet.empty()) {
                                CHDEBUG("remote end closed connection in conn for " << chan->_endpoint.GetURL());
                                return; // just say we're done so the loop can evaluate the end condition
                            }
                            CHDEBUG("Read "<< packet.size());
                            chan->_rpcParser.Feed(std::move(packet));
                            // process some messages from the packet
                            chan->_rpcParser.DispatchSome();
                        }
                    });
            }
            else{
                return seastar::make_ready_future<>();
            }
        }
    )
    .finally([chan=weak_from_this()] {
        if (chan) {
            CHDEBUG("closing channel...");
            return chan->_out.flush().then([chan=chan->weak_from_this()] () {
                if (chan) {
                    chan->_out.close();
                }
            });
        }
        else {
            return seastar::make_ready_future<>();
        }
    }); // finally
}

void TCPRPCChannel::RegisterMessageObserver(MessageObserver_t observer) {
    CDEBUG("register msg observer");
    if (observer == nullptr) {
        CDEBUG("Setting default message observer");
        _messageObserver = [this](Request request) {
            CWARN("Message: " << request.verb
               << " ignored since there is no message observer registered...");
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
        _failureObserver = [this](Endpoint& endpoint, std::exception_ptr exc) {
            CWARN("Ignoring failure: " << exc << ", from " << endpoint.GetURL()
                  << ", since there is no failure observer registered...");
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
    _pendingWrites.clear();
    _pendingWrites.resize(0); // reclaim any memory used by the vector
}

seastar::future<> TCPRPCChannel::GracefulClose(Duration timeout) {
    // TODO, this is ignored for now since the seastar shutdown* api doesn't return futures
    // and so we we can't setup a timeout on shutdown()
    (void) timeout;
    CDEBUG("graceful close")
    if(_fdIsSet) {
        _fd.shutdown_input();
        _fd.shutdown_output();
    }
    return seastar::make_ready_future<>();
}

} // k2tx
