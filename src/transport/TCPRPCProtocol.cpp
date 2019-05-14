//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCProtocol.h"

// third-party
#include <seastar/core/future-util.hh>
#include <arpa/inet.h> // for inet_ntop
#include <seastar/net/inet_address.hh> // for inet_address

//k2
#include "common/Log.h"

namespace k2 {
const String TCPRPCProtocol::proto("tcp+k2rpc");

TCPRPCProtocol::TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet, SocketAddress addr):
    IRPCProtocol(vnet, proto),
    _addr(addr),
    _stopped(true) {
    K2DEBUG("ctor");
}

TCPRPCProtocol::~TCPRPCProtocol() {
    K2DEBUG("dtor");
}

void TCPRPCProtocol::start() {
    K2DEBUG("start");
    seastar::listen_options lo;
    lo.reuse_address = true;
    _listen_socket = _vnet.local().listenTCP(_addr, lo);
    _stopped = false;

    seastar::do_until(
        [this] { return _stopped;},
        [this] {
        return _listen_socket->accept().then(
            [this] (seastar::connected_socket fd, SocketAddress addr) mutable {
                K2DEBUG("Accepted connection from " << addr);
                auto&& chan = seastar::make_lw_shared<TCPRPCChannel>
                                    (std::move(fd), _endpointFromAddress(std::move(addr)));
                _handleNewChannel(chan);
                return seastar::make_ready_future();
            }
        )
        .handle_exception([this] (auto exc) {
            if (!_stopped) {
                K2WARN("Accept received exception(ignoring): " << exc);
            }
            else {
                // let the loop keep going. The _stopped flag above will cause it to break
                K2DEBUG("Server is exiting...");
            }
            return seastar::make_ready_future();
        });
    }).or_terminate();
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, uint16_t port) {
    K2DEBUG("builder creating");
    return [&vnet, port]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2DEBUG("builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet, port));
    };
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, IAddressProvider& addrProvider) {
    K2DEBUG("builder creating");
    return [&vnet, &addrProvider]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        auto myID = seastar::engine().cpu_id() % seastar::smp::count;
        K2DEBUG("builder created");

        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet, addrProvider.getAddress(myID)));
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
        // we're about to kill this so unregister observers
        chan->registerFailureObserver(nullptr);
        chan->registerMessageObserver(nullptr);

        futs.push_back(chan->gracefulClose().then([chan](){}));
    }

    // here we return a future which completes once all GracefulClose futures complete.
    return seastar::when_all(futs.begin(), futs.end()).
        then([] (std::vector<seastar::future<>>) {
            return seastar::make_ready_future<>();
        });
}

std::unique_ptr<TXEndpoint> TCPRPCProtocol::getTXEndpoint(String url) {
    if (_stopped) {
        K2WARN("Unable to create endpoint since we're stopped for url " << url);
        return nullptr;
    }
    K2DEBUG("get endpoint for " << url);
    auto ep = TXEndpoint::fromURL(url, _vnet.local().getTCPAllocator());
    if (!ep || ep->getProtocol() != proto) {
        K2WARN("Cannot construct non-`" << proto << "` endpoint");
        return nullptr;
    }
    return std::move(ep);
}

void TCPRPCProtocol::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata metadata) {
    if (_stopped) {
        K2WARN("Dropping message since we're stopped: verb=" << verb << ", url=" << endpoint.getURL());
        return;
    }

    auto&& chan = _getOrMakeChannel(endpoint);
    if (!chan) {
        K2WARN("Dropping message: Unable to create connection for endpoint " << endpoint.getURL());
        return;
    }
    chan->send(verb, std::move(payload), std::move(metadata));
}

seastar::lw_shared_ptr<TCPRPCChannel> TCPRPCProtocol::_getOrMakeChannel(TXEndpoint& endpoint) {
    // look for an existing channel
    K2DEBUG("get or make channel: " << endpoint.getURL());
    auto iter = _channels.find(endpoint);
    if (iter != _channels.end()) {
        K2DEBUG("found existing channel");
        return iter->second;
    }
    K2DEBUG("creating new channel");

    // TODO support for IPv6?
    // TODO support for binding to local port
    auto address = seastar::make_ipv4_address({endpoint.getIP().c_str(), uint16_t(endpoint.getPort())});

    // we can only get a future for a connection at some point.
    auto futureConn = _vnet.local().connectTCP(address);
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
    K2DEBUG("processing channel: "<< chan->getTXEndpoint().getURL());

    chan->registerMessageObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (Request& request) {
        K2DEBUG("Message " << request.verb << " received from " << request.endpoint.getURL());
        seastar::weak_ptr<TCPRPCProtocol>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP && !weakP->_stopped) {
            weakP->_messageObserver(request);
        }
    });

    chan->registerFailureObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (TXEndpoint& endpoint, auto exc) {
        seastar::weak_ptr<TCPRPCProtocol>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP && !weakP->_stopped) {
            if (exc) {
                K2WARN("Channel " << endpoint.getURL() << ", failed due to " << exc);
            }
            auto chanIter = weakP->_channels.find(endpoint);
            if (chanIter != weakP->_channels.end()) {
                auto chan = chanIter->second;
                weakP->_channels.erase(chanIter);
                chan->gracefulClose().then([chan] {});
            }
        }
    });
    assert(chan->getTXEndpoint().canAllocate());
    _channels.emplace(chan->getTXEndpoint(), chan);
}

TXEndpoint TCPRPCProtocol::_endpointFromAddress(SocketAddress addr) {
    const size_t bufsize = 64;
    char buffer[bufsize];
    auto inetaddr=addr.addr();
    String ip(::inet_ntop(int(inetaddr.in_family()), inetaddr.data(), buffer, bufsize));
    return TXEndpoint(proto, std::move(ip), addr.port(), _vnet.local().getTCPAllocator());
}

} // namespace k2
