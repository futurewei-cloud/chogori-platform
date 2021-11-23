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

#include "RRDMARPCProtocol.h"

// third-party
#include <seastar/core/future-util.hh>

//k2
#include "Log.h"

namespace k2 {

RRDMARPCProtocol::RRDMARPCProtocol(VirtualNetworkStack::Dist_t& vnet):
    IRPCProtocol(vnet, proto),
    _stopped(true),
    _svrEndpoint(seastar::engine()._rdma_stack ?
            seastar::make_lw_shared<TXEndpoint>(_endpointFromAddress(seastar::engine()._rdma_stack->localEndpoint)) :
            nullptr)
    {
    K2LOG_D(log::tx, "ctor");
}

RRDMARPCProtocol::~RRDMARPCProtocol() {
    K2LOG_D(log::tx, "dtor");
}

void RRDMARPCProtocol::start() {
    K2LOG_D(log::tx, "start");
    if (_svrEndpoint) {
        _listener = _vnet.local().listenRRDMA();
        K2LOG_I(log::tx, "Starting listening RRDMA Proto on: {}", _svrEndpoint->url);

        _stopped = false;

        _listenerClosed = seastar::do_until(
            [this] { return _stopped;},
            [this] {
            return _listener.accept().then(
                [this] (std::unique_ptr<seastar::rdma::RDMAConnection>&& rconn) {
                    // Unlike TCP, RDMA uses symmetric endpoints for connecting and
                    // receiving connections. This causes channels to be dropped in the map.
                    // The top 8 bits of QP numbers are unused, so we shift the UDQP num
                    // to get unique endpoints
                    auto adjusted_addr = rconn->getAddr();
                    adjusted_addr.UDQP = adjusted_addr.UDQP << 8;

                    auto && ep = _endpointFromAddress(adjusted_addr);
                    K2LOG_D(log::tx, "Accepted connection from {}", ep.url);
                    _handleNewChannel(std::move(rconn), std::move(ep));
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
}

RPCProtocolFactory::BuilderFunc_t RRDMARPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet) {
    K2LOG_D(log::tx, "builder creating");
    return [&vnet]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2LOG_D(log::tx, "builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<RRDMARPCProtocol>(vnet));
    };
}

seastar::future<> RRDMARPCProtocol::stop() {
    K2LOG_D(log::tx, "stop");
    // immediately prevent accepting further read/write work
    _stopped = true;

    // place all channels in a list so that we can clear the map
    std::vector<seastar::lw_shared_ptr<RRDMARPCChannel>> channels;
    for (auto&& iter: _channels) {
        channels.push_back(iter.second);
    }
    _channels.clear();

    // now schedule futures for graceful close of all channels
    std::vector<seastar::future<>> futs;
    if (_svrEndpoint) futs.push_back(_listener.close());
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

std::unique_ptr<TXEndpoint> RRDMARPCProtocol::getTXEndpoint(String url) {
    if (_stopped) {
        K2LOG_W(log::tx, "Unable to create endpoint since we're stopped for url {}", url);
        return nullptr;
    }
    auto ep = TXEndpoint::fromURL(url, _vnet.local().getRRDMAAllocator());
    if (!ep || ep->protocol != proto) {
        K2LOG_W(log::tx, "Cannot construct non-`{}` endpoint from url {}", proto, url);
        return nullptr;
    }
    return ep;
}

seastar::lw_shared_ptr<TXEndpoint> RRDMARPCProtocol::getServerEndpoint() {
    return _svrEndpoint;
}

void RRDMARPCProtocol::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata metadata) {
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

seastar::lw_shared_ptr<RRDMARPCChannel> RRDMARPCProtocol::_getOrMakeChannel(TXEndpoint& endpoint) {
    // look for an existing channel
    auto iter = _channels.find(endpoint);
    if (iter != _channels.end()) {
        return iter->second;
    }

    if (seastar::engine()._rdma_stack) {
        K2LOG_D(log::tx, "creating new channel to {}", endpoint.url);
        auto address = seastar::rdma::EndPoint(endpoint.ip, endpoint.port);
        auto rconn = _vnet.local().connectRRDMA(address);

        // wrap the connection into a RRDMAChannel
        return _handleNewChannel(std::move(rconn), endpoint);
    }
    return nullptr;
}

seastar::lw_shared_ptr<RRDMARPCChannel>
RRDMARPCProtocol::_handleNewChannel(std::unique_ptr<seastar::rdma::RDMAConnection> rconn, TXEndpoint endpoint) {
    K2LOG_D(log::tx, "processing channel: {}", endpoint.url);
    auto chan = seastar::make_lw_shared<RRDMARPCChannel>(std::move(rconn), std::move(endpoint),
        [this] (Request&& request) {
            K2LOG_D(log::tx, "Message verb={}, url={} received", request.verb, request.endpoint.url);
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
    K2ASSERT(log::tx, chan->getTXEndpoint().canAllocate(), "endpoint cannot allocate");
    auto [it, placed] = _channels.emplace(chan->getTXEndpoint(), chan);
    if (!placed) {
        K2LOG_W(log::tx, "Failed to place new RDMA channel");
    }
    chan->run();
    return chan;
}

TXEndpoint RRDMARPCProtocol::_endpointFromAddress(seastar::rdma::EndPoint addr) {
    return TXEndpoint(String(proto), seastar::rdma::EndPoint::GIDToString(addr.GID), addr.UDQP, _vnet.local().getRRDMAAllocator());
}

} // namespace k2
