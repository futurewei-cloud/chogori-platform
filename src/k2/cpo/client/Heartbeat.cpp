/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include "Heartbeat.h"

#include <k2/cpo/client/CPOClient.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/transport/TCPRPCProtocol.h>
#include <k2/transport/RRDMARPCProtocol.h>

namespace k2 {

void HeartbeatResponder::_registerMetrics() {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;
    labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

    _metric_groups.add_group("HeartbeatResponder", {
        sm::make_counter("heartbeats", _heartbeats, sm::description("Number of heartbeat requests"), labels),
    });
}

seastar::future<std::tuple<Status, dto::HeartbeatResponse>>
HeartbeatResponder::_handleHeartbeat(dto::HeartbeatRequest&& request) {
    K2LOG_V(log::cpoclient, "HB Request {}", request);
    _heartbeats++;

    if (!_up) {
        // First heartbeat we have seen, initialize the HB config
        _HBInterval = request.interval;
        _HBDeadThreshold = request.deadThreshold;
        _up = true;
    } else if (_lastSeq != request.lastToken) {
        // Monitor did not get our response
        ++_missedHBs;
        K2LOG_I(log::cpoclient, "CPO did not get last response (token mismatch)");
    }

    if (_missedHBs >= _HBDeadThreshold - 1) {
        if (_up) {
            K2LOG_W(log::cpoclient, "Too many heartbeats missed, this node is considered down");
            _up = false;
        }
        K2LOG_W(log::cpoclient, "This node is already considered down");
        return RPCResponse(Statuses::S410_Gone("This target is down due to lack of heartbeats"),
                                                dto::HeartbeatResponse{});
    }

    return _nextHeartbeatExpire.stop()
    .then([this] () {
        _lastSeq++;
        _missedHBs = 0;
        dto::HeartbeatResponse response {
            _ID,
            _metadata,
            _eps,
            _lastSeq
        };

        _nextHeartbeatExpire.arm(_HBInterval + (_HBInterval / 2));
        return RPCResponse(Statuses::S200_OK("Heartbeat success"), std::move(response));
    });
}

seastar::future<> HeartbeatResponder::gracefulStop() {
    RPC().registerMessageObserver(dto::Verbs::CPO_HEARTBEAT, nullptr);

    return _nextHeartbeatExpire.stop();
}

seastar::future<> HeartbeatResponder::start() {
    auto tcp_ep = k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto);
    if (tcp_ep) {
        _ID = tcp_ep->url;
        _eps.push_back(tcp_ep->url);
    }
    auto rdma_ep = k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto);
    if (rdma_ep) {
        _eps.push_back(tcp_ep->url);
    }

    _registerMetrics();

    _nextHeartbeatExpire.setCallback([this] () {
        ++_missedHBs;
        K2LOG_D(log::cpoclient, "Heartbeat from CPO monitor was missed");
        if (_missedHBs >= _HBDeadThreshold - 1) {
            K2LOG_W(log::cpoclient, "Too many heartbeats missed, this node is considered down");
            _up = false;
        }

        if (_up) {
            _nextHeartbeatExpire.arm(_HBInterval);
        }
    });

    RPC().registerRPCObserver<dto::HeartbeatRequest, dto::HeartbeatResponse>
    (dto::Verbs::CPO_HEARTBEAT, [this](dto::HeartbeatRequest&& request) {
        return _handleHeartbeat(std::move(request));
    });

    return seastar::make_ready_future<>();
}

// Used by other applets to know if they should soft-down themselves
bool HeartbeatResponder::isUp() {
    return _up;
}

// Used by other applets to set role-specific metadata,
// which is passed on to the monitor in the HB response
void HeartbeatResponder::setRoleMetadata(String metadata) {
    _metadata = metadata;
}

} // namespace k2
