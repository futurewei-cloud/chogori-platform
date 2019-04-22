//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCProtocol.h"

// third-party
#include <seastar/core/future-util.hh>
#include <arpa/inet.h> // for inet_ntop
#include <seastar/net/inet_address.hh> // for inet_address

//k2tx
#include "Log.h"

namespace k2tx {
const String TCPRPCProtocol::proto("tcp+k2rpc");

TCPRPCProtocol::TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet, uint16_t port):
    IRPCProtocol(vnet, proto),
    _port(port),
    _stopped(true) {
    K2DEBUG("ctor");
}

TCPRPCProtocol::~TCPRPCProtocol() {
    K2DEBUG("dtor");
}

void TCPRPCProtocol::Start() {
    K2DEBUG("start");
    seastar::listen_options lo;
    lo.reuse_address = true;
    _listen_socket = _vnet.local().ListenTCP(seastar::make_ipv4_address({_port}), lo);
    _stopped = false;

    seastar::do_until(
        [this] { return _stopped;},
        [this] {
        return _listen_socket->accept().then(
            [this] (seastar::connected_socket fd, seastar::socket_address addr) mutable {
                K2DEBUG("Accepted connection from " << addr);
                auto&& chan = seastar::make_lw_shared<TCPRPCChannel>
                                    (std::move(fd), _endpointFromAddress(std::move(addr)));
                _handleNewChannel(chan);
            }
        )
        .handle_exception([this] (auto exc) {
            if (!_stopped) {
                K2WARN("Accept received exception(ignoring): " << exc);
            }
            else {
                K2DEBUG("Server is exiting...");
            }
            return seastar::make_ready_future();
        });
    }).or_terminate();
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::Builder(VirtualNetworkStack::Dist_t& vnet, uint16_t port) {
    K2DEBUG("builder creating");
    return [&vnet, port]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2DEBUG("builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet, port));
    };
}

seastar::future<> TCPRPCProtocol::stop() {
    K2DEBUG("stop");
    // immediately prevent accepting further read/write work
    _stopped = true;
    _listen_socket->abort_accept();

    // place all channels in a list so that we can clear the map
    std::vector<seastar::lw_shared_ptr<TCPRPCChannel>> channels;
    for (auto&& iter: _channels) {
        channels.push_back(iter.second);
    }
    _channels.clear();

    // now schedule futures for graceful close of all channels
    std::vector<seastar::future<>> futs;
    for (auto chan: channels) {
        // schedule a graceful close. Note the empty continuation which captures the shared pointer to the channel
        // by copy so that the channel isn't going to get destroyed mid-sentence
        futs.push_back(chan->GracefulClose().then([chan](){}));
    }

    // here we return a future which completes once all GracefulClose futures complete.
    return seastar::when_all(futs.begin(), futs.end()).
        then([] (std::vector<seastar::future<>>) {
            return seastar::make_ready_future<>();
        });
}

std::unique_ptr<Endpoint> TCPRPCProtocol::GetEndpoint(String url) {
    K2DEBUG("Get endpoint for " << url);
    auto ep = Endpoint::FromURL(url, _vnet.local().GetTCPAllocator());
    if (!ep || ep->GetProtocol() != proto) {
        K2WARN("Cannot construct non-`" << proto << "` endpoint");
        return nullptr;
    }
    return std::move(ep);
}

void TCPRPCProtocol::Send(Verb verb, std::unique_ptr<Payload> payload, Endpoint& endpoint, MessageMetadata metadata) {
    if (_stopped) {
        K2WARN("Dropping message since we're stopped: verb=" << verb << ", url=" << endpoint.GetURL());
        return;
    }

    auto&& chan = _getOrMakeChannel(endpoint);
    if (!chan) {
        K2WARN("Dropping message: Unable to create connection for endpoint " << endpoint.GetURL());
        return;
    }
    chan->Send(verb, std::move(payload), std::move(metadata));
}

seastar::lw_shared_ptr<TCPRPCChannel> TCPRPCProtocol::_getOrMakeChannel(Endpoint& endpoint) {
    // look for an existing channel
    K2DEBUG("get or make channel: " << endpoint.GetURL());
    auto iter = _channels.find(endpoint);
    if (iter != _channels.end()) {
        K2DEBUG("found existing channel");
        return iter->second;
    }
    K2DEBUG("creating new channel");

    // TODO support for IPv6?
    // TODO support for binding to local port
    auto address = seastar::make_ipv4_address({endpoint.GetIP().c_str(), uint16_t(endpoint.GetPort())});

    // we can only get a future for a connection at some point.
    auto futureConn = _vnet.local().ConnectTCP(address);
    if (futureConn.failed()) {
        // the conn failed immediately
        return nullptr;
    }
    // wrap the connection into a TCPChannel
    auto chan = seastar::make_lw_shared<TCPRPCChannel>(std::move(futureConn), endpoint);
    _handleNewChannel(chan);
    return chan;
}

void TCPRPCProtocol::_handleNewChannel(seastar::lw_shared_ptr<TCPRPCChannel> chan) {
    if (!chan) {
        K2WARN("skipping processing of an empty channel");
        return;
    }
    K2DEBUG("processing channel: "<< chan->GetEndpoint().GetURL());

    chan->RegisterMessageObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (Request request) {
        K2DEBUG("Message " << request.verb << " received from " << request.endpoint.GetURL());
        auto weakP = shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP->get() && !weakP->get()->_stopped) {
            weakP->get()->_messageObserver(std::move(request));
        }
    });

    chan->RegisterFailureObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (Endpoint& endpoint, auto exc) {
        K2WARN("Channel " << endpoint.GetURL() << ", failed due to " << exc);
        auto weakP = shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP->get() && !weakP->get()->_stopped) {
            weakP->get()->_channels.erase(endpoint);
        }
    });

    _channels.emplace(chan->GetEndpoint(), chan);
}

Endpoint TCPRPCProtocol::_endpointFromAddress(seastar::socket_address addr) {
    const size_t bufsize = 64;
    char buffer[bufsize];
    auto inetaddr=addr.addr();
    String ip(::inet_ntop(int(inetaddr.in_family()), inetaddr.data(), buffer, bufsize));
    return Endpoint(proto, std::move(ip), addr.port(), _vnet.local().GetTCPAllocator());
}

} // namespace k2tx
