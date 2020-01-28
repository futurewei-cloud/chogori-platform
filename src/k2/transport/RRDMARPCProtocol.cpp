//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "RRDMARPCProtocol.h"

// third-party
#include <seastar/core/future-util.hh>

//k2
#include <k2/common/Log.h>

namespace k2 {
const String RRDMARPCProtocol::proto("rrdma+k2rpc");

RRDMARPCProtocol::RRDMARPCProtocol(VirtualNetworkStack::Dist_t& vnet):
    IRPCProtocol(vnet, proto),
    _stopped(true),
    _svrEndpoint(seastar::make_lw_shared<TXEndpoint>(
            _endpointFromAddress(seastar::engine()._rdma_stack ?
                                seastar::engine()._rdma_stack->localEndpoint:
                                seastar::rdma::EndPoint{}))
    ) {
    K2DEBUG("ctor");
}

RRDMARPCProtocol::~RRDMARPCProtocol() {
    K2DEBUG("dtor");
}

void RRDMARPCProtocol::start() {
    K2DEBUG("start");
    if (seastar::engine()._rdma_stack) {
        _listener = _vnet.local().listenRRDMA();
        K2INFO("Starting listening RRDMA Proto on: " << _svrEndpoint->getURL());

        _stopped = false;

        _listenerClosed = seastar::do_until(
            [this] { return _stopped;},
            [this] {
            return _listener.accept().then(
                [this] (std::unique_ptr<seastar::rdma::RDMAConnection> rconn) {
                    auto && ep = _endpointFromAddress(rconn->getAddr());
                    K2DEBUG("Accepted connection from " << ep.getURL());
                    _handleNewChannel(std::move(rconn), std::move(ep));
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
}

RPCProtocolFactory::BuilderFunc_t RRDMARPCProtocol::builder(VirtualNetworkStack::Dist_t& vnet) {
    K2DEBUG("builder creating");
    return [&vnet]() mutable -> seastar::shared_ptr<IRPCProtocol> {
        K2DEBUG("builder running");
        return seastar::static_pointer_cast<IRPCProtocol>(
            seastar::make_shared<RRDMARPCProtocol>(vnet));
    };
}

seastar::future<> RRDMARPCProtocol::stop() {
    K2DEBUG("stop");
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
    futs.push_back(_listener.close());
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
    return seastar::when_all(futs.begin(), futs.end()).discard_result();
}

std::unique_ptr<TXEndpoint> RRDMARPCProtocol::getTXEndpoint(String url) {
    if (_stopped) {
        K2WARN("Unable to create endpoint since we're stopped for url " << url);
        return nullptr;
    }
    K2DEBUG("get endpoint for " << url);
    auto ep = TXEndpoint::fromURL(url, _vnet.local().getRRDMAAllocator());
    if (!ep || ep->getProtocol() != proto) {
        K2WARN("Cannot construct non-`" << proto << "` endpoint");
        return nullptr;
    }
    return std::move(ep);
}

seastar::lw_shared_ptr<TXEndpoint> RRDMARPCProtocol::getServerEndpoint() {
    return _svrEndpoint;
}

void RRDMARPCProtocol::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata metadata) {
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

seastar::lw_shared_ptr<RRDMARPCChannel> RRDMARPCProtocol::_getOrMakeChannel(TXEndpoint& endpoint) {
    // look for an existing channel
    K2DEBUG("get or make channel: " << endpoint.getURL());
    auto iter = _channels.find(endpoint);
    if (iter != _channels.end()) {
        K2DEBUG("found existing channel");
        return iter->second;
    }

    if (seastar::engine()._rdma_stack) {
        K2DEBUG("creating new channel");
        auto address = seastar::rdma::EndPoint(endpoint.getIP(), endpoint.getPort());
        auto rconn = _vnet.local().connectRRDMA(address);

        // wrap the connection into a RRDMAChannel
        return _handleNewChannel(std::move(rconn), endpoint);
    }
    return nullptr;
}

seastar::lw_shared_ptr<RRDMARPCChannel>
RRDMARPCProtocol::_handleNewChannel(std::unique_ptr<seastar::rdma::RDMAConnection> rconn, TXEndpoint endpoint) {
    K2DEBUG("processing channel: "<< endpoint.getURL());
    auto chan = seastar::make_lw_shared<RRDMARPCChannel>(std::move(rconn), std::move(endpoint),
        [this] (Request&& request) {
            K2DEBUG("Message " << request.verb << " received from " << request.endpoint.getURL());
            if (!_stopped) {
                _messageObserver(std::move(request));
            }
        },
        [this] (TXEndpoint& endpoint, auto exc) {
            if (!_stopped) {
                if (exc) {
                    K2WARN("Channel " << endpoint.getURL() << ", failed due to " << exc);
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
    assert(chan->getTXEndpoint().canAllocate());
    _channels.emplace(chan->getTXEndpoint(), chan);
    chan->run();
    return chan;
}

TXEndpoint RRDMARPCProtocol::_endpointFromAddress(seastar::rdma::EndPoint addr) {
    return TXEndpoint(String(proto), seastar::rdma::EndPoint::GIDToString(addr.GID), addr.UDQP, _vnet.local().getRRDMAAllocator());
}

} // namespace k2
