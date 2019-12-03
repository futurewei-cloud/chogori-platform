//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "TCPRPCProtocol.h"

// third-party
#include <seastar/core/future-util.hh>
#include <seastar/net/api.hh>
#include <arpa/inet.h> // for inet_ntop
#include <seastar/net/inet_address.hh> // for inet_address

//k2
#include <k2/common/Log.h>

namespace k2 {
const String TCPRPCProtocol::proto("tcp+k2rpc");

TCPRPCProtocol::TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet):
    IRPCProtocol(vnet, proto),
    _stopped(true) {
    K2DEBUG("ctor");
}

TCPRPCProtocol::TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet, SocketAddress addr):
    IRPCProtocol(vnet, proto),
    _addr(addr),
    _svrEndpoint(seastar::make_lw_shared<TXEndpoint>(_endpointFromAddress(_addr))),
    _stopped(true) {
    K2DEBUG("ctor");
}

TCPRPCProtocol::~TCPRPCProtocol() {
    K2DEBUG("dtor");
}

void TCPRPCProtocol::start() {
    K2DEBUG("start");
    _stopped = false;
    if (_svrEndpoint) {
        K2INFO("Starting listening TCP Proto on: " << _svrEndpoint->getURL());

        seastar::listen_options lo;
        lo.reuse_address = true;
        lo.lba = seastar::server_socket::load_balancing_algorithm::port;
        _listen_socket = _vnet.local().listenTCP(_addr, lo);
        if (_addr.port() == 0) {
            // update the local endpoint if we're binding to port 0
            _svrEndpoint = seastar::make_lw_shared<>(_endpointFromAddress(_listen_socket->local_address()));
            K2INFO("Effective listening TCP Proto on: " << _svrEndpoint->getURL());
        }

        (void) seastar::do_until(
            [this] { return _stopped;},
            [this] {
            return _listen_socket->accept().then(
                [this] (seastar::accept_result result) {
                    K2DEBUG("Accepted connection from " << addr);
                    _handleNewChannel(seastar::make_ready_future<seastar::connected_socket>(std::move(result.connection)),  _endpointFromAddress(std::move(result.remote_address)));
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
    else {
        K2INFO("Starting non-listening TCP Proto...");
    }
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet) {
    K2DEBUG("builder creating non-listening tcp protocol");
    return [&vnet]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2DEBUG("builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet));
    };
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, uint16_t port) {
    K2DEBUG("builder creating tcp protocol on port " << port);
    return [&vnet, port]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2DEBUG("builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet, port));
    };
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, IAddressProvider& addrProvider) {
    K2DEBUG("builder creating multi-address tcp protocol");
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
    if (_listen_socket) {
        _listen_socket->abort_accept();
    }

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
            return seastar::make_ready_future();
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

seastar::lw_shared_ptr<TXEndpoint> TCPRPCProtocol::getServerEndpoint() {
    return _svrEndpoint;
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
    return _handleNewChannel(std::move(futureConn), endpoint);
}

seastar::lw_shared_ptr<TCPRPCChannel>
TCPRPCProtocol::_handleNewChannel(seastar::future<seastar::connected_socket> futureSocket, const TXEndpoint& endpoint) {
    K2DEBUG("processing channel: "<< endpoint.getURL());
    auto chan = seastar::make_lw_shared<TCPRPCChannel>(std::move(futureSocket), endpoint,
        [shptr=seastar::make_lw_shared<>(weak_from_this())] (Request&& request) {
            K2DEBUG("Message " << request.verb << " received from " << request.endpoint.getURL());
            seastar::weak_ptr<TCPRPCProtocol>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
            if (weakP && !weakP->_stopped) {
                weakP->_messageObserver(std::move(request));
            }
        },
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
                    return chan->gracefulClose().then([chan] {});
                }
            }
            return seastar::make_ready_future();
        });
    assert(chan->getTXEndpoint().canAllocate());
    _channels.emplace(chan->getTXEndpoint(), chan);
    chan->run();
    return chan;
}

TXEndpoint TCPRPCProtocol::_endpointFromAddress(SocketAddress addr) {
    const size_t bufsize = 64;
    char buffer[bufsize];
    auto inetaddr=addr.addr();
    String ip(::inet_ntop(int(inetaddr.in_family()), inetaddr.data(), buffer, bufsize));
    return TXEndpoint(proto, std::move(ip), addr.port(), _vnet.local().getTCPAllocator());
}

} // namespace k2
