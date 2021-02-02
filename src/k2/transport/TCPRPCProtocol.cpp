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

#include "TCPRPCProtocol.h"

// third-party
#include <seastar/core/future-util.hh>
#include <seastar/net/api.hh>
#include <arpa/inet.h> // for inet_ntop
#include <seastar/net/inet_address.hh> // for inet_address

//k2
#include "Log.h"

namespace k2 {

TCPRPCProtocol::TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet):
    IRPCProtocol(vnet, proto),
    _stopped(true) {
    K2LOG_D(log::tx, "ctor");
}

TCPRPCProtocol::TCPRPCProtocol(VirtualNetworkStack::Dist_t& vnet, SocketAddress addr):
    IRPCProtocol(vnet, proto),
    _addr(addr),
    _svrEndpoint(seastar::make_lw_shared<TXEndpoint>(_endpointFromAddress(_addr))),
    _stopped(true) {
    K2LOG_D(log::tx, "ctor");
}

TCPRPCProtocol::~TCPRPCProtocol() {
    K2LOG_D(log::tx, "dtor");
}

void TCPRPCProtocol::start() {
    K2LOG_D(log::tx, "start");
    _stopped = false;
    if (_svrEndpoint) {
        K2LOG_I(log::tx, "Starting listening TCP Proto on: {}", _svrEndpoint->url);

        seastar::listen_options lo;
        lo.reuse_address = true;
        lo.lba = seastar::server_socket::load_balancing_algorithm::port;
        _listen_socket = _vnet.local().listenTCP(_addr, lo);
        if (_addr.port() == 0) {
            // update the local endpoint if we're binding to port 0
            _svrEndpoint = seastar::make_lw_shared<>(_endpointFromAddress(_listen_socket->local_address()));
            K2LOG_I(log::tx, "Effective listening TCP Proto on: {}", _svrEndpoint->url);
        }

        _listenerClosed = seastar::do_until(
            [this] { return _stopped;},
            [this] {
            return _listen_socket->accept().then(
                [this] (seastar::accept_result&& result) {
                    K2LOG_D(log::tx, "Accepted connection from {}", result.remote_address);
                    _handleNewChannel(seastar::make_ready_future<seastar::connected_socket>(std::move(result.connection)),  _endpointFromAddress(std::move(result.remote_address)));
                    return seastar::make_ready_future();
                }
            )
            .handle_exception([this] (auto exc) {
                if (!_stopped) {
                    K2LOG_W_EXC(log::tx, exc, "Accept received exception(ignoring)");
                }
                else {
                    // let the loop keep going. The _stopped flag above will cause it to break
                    K2LOG_D(log::tx, "Server is exiting...");
                }
                return seastar::make_ready_future();
            });
        }).or_terminate();
    }
    else {
        K2LOG_I(log::tx, "Starting non-listening TCP Proto...");
    }
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet) {
    K2LOG_D(log::tx, "builder creating non-listening tcp protocol");
    return [&vnet]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2LOG_D(log::tx, "builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet));
    };
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, uint16_t port) {
    K2LOG_D(log::tx, "builder creating tcp protocol on port {}", port);
    return [&vnet, port]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2LOG_D(log::tx, "builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet, port));
    };
}

RPCProtocolFactory::BuilderFunc_t TCPRPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet, IAddressProvider& addrProvider) {
    K2LOG_D(log::tx, "builder creating multi-address tcp protocol");
    return [&vnet, &addrProvider]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        auto myID = seastar::this_shard_id() % seastar::smp::count;
        K2LOG_D(log::tx, "builder created");

        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<TCPRPCProtocol>(vnet, addrProvider.getAddress(myID)));
    };
}

seastar::future<> TCPRPCProtocol::stop() {
    K2LOG_D(log::tx, "stop");
    // immediately prevent accepting further read/write work
    _stopped = true;
    if (_listen_socket) {
        _listen_socket.release();
    }

    // place all channels in a list so that we can clear the map
    std::vector<seastar::lw_shared_ptr<TCPRPCChannel>> channels;
    for (auto&& iter: _channels) {
        channels.push_back(iter.second);
    }
    _channels.clear();

    // now schedule futures for graceful close of all channels
    std::vector<seastar::future<>> futs;
    futs.push_back(std::move(_listenerClosed));
    for (auto chan: channels) {
        // schedule a graceful close. Note the empty continuation which captures the shared pointer to the channel
        // by copy so that the channel isn't going to get destroyed mid-sentence
        // we're about to kill this so unregister observers
        chan->registerFailureObserver(nullptr);
        chan->registerMessageObserver(nullptr);

        futs.push_back(chan->gracefulClose().then([chan](){}));
    }

    // here we return a future which completes once all GracefulClose futures complete.
    return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result();
}

std::unique_ptr<TXEndpoint> TCPRPCProtocol::getTXEndpoint(String url) {
    if (_stopped) {
        K2LOG_W(log::tx, "Unable to create endpoint since we're stopped for url {}", url);
        return nullptr;
    }
    auto ep = TXEndpoint::fromURL(url, _vnet.local().getTCPAllocator());
    if (!ep || ep->protocol != proto) {
        K2LOG_W(log::tx, "Cannot construct non-`{}` endpoint", proto);
        return nullptr;
    }
    return ep;
}

seastar::lw_shared_ptr<TXEndpoint> TCPRPCProtocol::getServerEndpoint() {
    return _svrEndpoint;
}

void TCPRPCProtocol::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata metadata) {
    if (_stopped) {
        K2LOG_W(log::tx, "Dropping message since we're stopped: verb={}, url={}", int(verb), endpoint.url);
        return;
    }

    auto&& chan = _getOrMakeChannel(endpoint);
    if (!chan) {
        K2LOG_W(log::tx, "Dropping message: Unable to create connection for endpoint {}", endpoint.url);
        return;
    }
    chan->send(verb, std::move(payload), std::move(metadata));
}

seastar::lw_shared_ptr<TCPRPCChannel> TCPRPCProtocol::_getOrMakeChannel(TXEndpoint& endpoint) {
    // look for an existing channel
    auto iter = _channels.find(endpoint);
    if (iter != _channels.end()) {
        return iter->second;
    }
    K2LOG_D(log::tx, "creating new channel for {}", endpoint.url);

    // TODO support for IPv6?
    auto address = seastar::make_ipv4_address({endpoint.ip.c_str(), uint16_t(endpoint.port)});

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
    K2LOG_D(log::tx, "processing channel: {}", endpoint.url);
    auto chan = seastar::make_lw_shared<TCPRPCChannel>(std::move(futureSocket), endpoint,
        [this] (Request&& request) {
            if (!_stopped) {
                _messageObserver(std::move(request));
            }
        },
        [this] (TXEndpoint& endpoint, auto exc) {
            if (!_stopped) {
                if (exc) {

                    K2LOG_W_EXC(log::tx, exc, "Channel {} failed", endpoint.url);
                }
                auto chanIter = _channels.find(endpoint);
                if (chanIter != _channels.end()) {
                    auto chan = chanIter->second;
                    _channels.erase(chanIter);
                    return chan->gracefulClose().then([chan] {});
                }
            }
            return seastar::make_ready_future();
        });
    _channels.emplace(chan->getTXEndpoint(), chan);
    chan->run();
    return chan;
}

TXEndpoint TCPRPCProtocol::_endpointFromAddress(SocketAddress addr) {
    const size_t bufsize = 64;
    char buffer[bufsize];
    auto inetaddr=addr.addr();
    String ip(::inet_ntop(int(inetaddr.in_family()), inetaddr.data(), buffer, bufsize));
    return TXEndpoint(String(proto), std::move(ip), addr.port(), _vnet.local().getTCPAllocator());
}

} // namespace k2
