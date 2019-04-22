//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCChannel.h"

// third-party
#include <seastar/net/inet_address.hh>

// k2tx
#include "Log.h"

#define CDEBUG(msg) K2DEBUG("{conn="<< (void*)this << ", addr=" << this->_endpoint.GetURL() << "} " << msg)
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
}

void TCPRPCChannel::Send(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata, bool flush) {
    if (!_fdIsSet) {
        // we don't have a connected socket yet. Queue up the request
        _pendingWrites.emplace_back(_BufferedWrite{verb, std::move(payload), std::move(metadata)});
        return;
    }
    // Messages are written in two parts: the header and the payload.
    // ask the serializer to write out its header to a fragment
    _out.write(RPCParser::SerializeHeader(_endpoint.NewFragment(), verb, std::move(metadata)));

    // payload is optional
    if (payload->Size() > 0) {
        // we have some payload to write
        auto& fragments = payload->Fragments();

        // write all but last fragment as-is
        for (size_t i = 0; i < fragments.size() - 1; ++i) {
            _out.write(std::move(fragments[i]));
        }
        if (payload->LastFragmentSize() > 0) {
            // we only write LastFragmentSize bytes from the last fragment
            fragments.back().trim(payload->LastFragmentSize());
            _out.write(std::move(fragments.back()));
        }
    }
    // at this point payload.Fragments() still has the same elements but they are empty. However we're done with payload,
    // so no need to do anything with that vector
    // RIP payload...
    if (flush) {
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
        [this] { return this->_in.eof(); }, // end condition for loop
        [this] { // body of loop
            if (this->_rpcParser.CanDispatch()) {
                CDEBUG("RPC parser can dispatch more messages as-is. not reading from socket this round")
                this->_rpcParser.DispatchSome();
                return seastar::make_ready_future<>();
            }
            return this->_in.read().then(
                [this](seastar::temporary_buffer<char> packet) {
                    if (packet.empty()) {
                        CDEBUG("remote end closed connection in conn for " << _endpoint.GetURL());
                        return; // just say we're done so the loop can evaluate the end condition
                    }
                    CDEBUG("Read "<< packet.size());
                    this->_rpcParser.Feed(std::move(packet));
                    // process some messages from the packet
                    this->_rpcParser.DispatchSome();
                });
        }
    )
    .finally([this] {
        CDEBUG("closing channel...");
        return this->_out.close();
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
