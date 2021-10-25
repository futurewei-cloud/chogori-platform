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

#include "HealthMonitor.h"

#include <k2/dto/MessageVerbs.h>

namespace k2 {

void HealthMonitor::addHBControl(RPCServer&& server, TimePoint nextHB) {
    auto control = seastar::make_lw_shared<HeartbeatControl>();
    control->target = std::move(server);
    control->endpoint = RPC().getTXEndpoint(control->target.ID);
    control->nextHeartbeat = nextHB;

    _heartbeats.push_back(std::move(control));
}

void HealthMonitor::checkHBs() {
    if (!_running) {
        return;
    }

    auto now = CachedSteadyClock::now(true);
    Duration nextArm = _interval();

    auto it = _heartbeats.begin();
    while (it != _heartbeats.end()) {
        // List is kept in sorted order, if we reach one that is not expired then we are done,
        // arm the timer and exit
        if ((*it)->nextHeartbeat > now) {
            nextArm = (*it)->nextHeartbeat - now;
            break;
        }

        // If the last heartbeat has not been responded to yet, count it as missed and potentially the target
        // as dead.
        if ((*it)->heartbeatInFlight) {
            K2LOG_D(log::cposvr, "missed HB for {}", *it);
            (*it)->unackedHeartbeats++;
            if ((*it)->unackedHeartbeats >= _deadThreshold()) {
                K2LOG_I(log::cposvr, "HB target is dead: {}", **it);
                _deadHeartbeats.push_back(std::move(*it));
                it = _heartbeats.erase(it);
                continue;
            }
        }

        // Normal case. Send a new heartbeat request to target.

        dto::HeartbeatRequest request{.lastToken = (*it)->lastToken, .interval = _interval(),
                                        .deadThreshold = _deadThreshold()};

        (*it)->heartbeatInFlight = true;
        (*it)->nextHeartbeat = now + _interval();

        (*it)->heartbeatRequest = RPC()
        .callRPC<dto::HeartbeatRequest, dto::HeartbeatResponse>(dto::Verbs::CPO_HEARTBEAT, request,
                                                                *((*it)->endpoint), _interval())
        .then([this, hb_ptr=*it, it] (auto&& result) {
            auto& [status, response] = result;
            K2LOG_V(log::cposvr, "Heartbeat response for {}", *hb_ptr);

            if (!status.is2xxOK()) {
                // Missed HB will be counted the next time this target reaches the head of the list
                return seastar::make_ready_future<>();
            }
            if (hb_ptr->unackedHeartbeats >= _deadThreshold()) {
                // This target has already been determined dead (and iterator is not valid anymore)
                K2LOG_I(log::cposvr, "HB response after target determined dead for {}", *hb_ptr);
                return seastar::make_ready_future<>();
            }

            hb_ptr->target.roleMetadata = std::move(response.roleMetadata);
            hb_ptr->target.endpoints = std::move(response.endpoints);

            hb_ptr->heartbeatInFlight = false;
            hb_ptr->unackedHeartbeats = 0;
            auto now = CachedSteadyClock::now(true);
            hb_ptr->nextHeartbeat = now + _interval();
            hb_ptr->lastToken = response.echoToken;

            _heartbeats.erase(it);
            _heartbeats.push_back(hb_ptr);

            return seastar::make_ready_future<>();
        })
        .handle_exception([hb_ptr=*it] (auto exc){
            K2LOG_W_EXC(log::cposvr, exc, "caught exception in heartbeat RPC for target {}", *hb_ptr);
            return seastar::make_ready_future();
        });
    }

    _nextHeartbeat.arm(nextArm);
}

seastar::future<> HealthMonitor::gracefulStop() {
     if (seastar::this_shard_id() != 1) {
        // Health monitor only runs on core 1
        return seastar::make_ready_future<>();
    }

    _running = false;
    seastar::future<> timer = _nextHeartbeat.stop();

    return seastar::do_with(std::vector<seastar::future<>>{}, [this, timer=std::move(timer)]
                                                              (std::vector<seastar::future<>>& futs) mutable {
        futs.reserve(_heartbeats.size() + _deadHeartbeats.size() + 1);

        futs.push_back(std::move(timer));

        for (auto hb : _heartbeats) {
            futs.push_back(std::move(hb->heartbeatRequest));
        }

        // Need to wait for dead heartbeats too because the RPC request could still be waiting for a timeout
        for (auto hb : _deadHeartbeats) {
            futs.push_back(std::move(hb->heartbeatRequest));
        }

        return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result();
    });
}

seastar::future<> HealthMonitor::start() {
    if (seastar::this_shard_id() != 1) {
        // Health monitor only runs on core 1
        return seastar::make_ready_future<>();
    }

    K2LOG_D(log::cposvr, "HealthMonitor start()");
    _running = true;

    auto now = CachedSteadyClock::now(true);
    uint32_t count = 0;
    Duration delay = 0s;

    for(const String& ep_url : _nodepoolEndpoints()) {
        RPCServer server;
        server.ID = ep_url;
        server.role = "Nodepool";

        addHBControl(std::move(server), now + delay);

        ++count;
        if (count == _batchSize()) {
            delay += _batchWait();
            count = 0;
        }
    }

    for(const String& ep_url : _TSOEndpoints()) {
        RPCServer server;
        server.ID = ep_url;
        server.role = "TSO";

        addHBControl(std::move(server), now + delay);

        ++count;
        if (count == _batchSize()) {
            delay += _batchWait();
            count = 0;
        }
    }

    for(const String& ep_url : _persistEndpoints()) {
        RPCServer server;
        server.ID = ep_url;
        server.role = "Persistence";

        addHBControl(std::move(server), now + delay);

        ++count;
        if (count == _batchSize()) {
            delay += _batchWait();
            count = 0;
        }
    }

    _nextHeartbeat.setCallback([this] {
        K2LOG_D(log::cposvr, "Heartbeat timer fired");
        checkHBs();
    });
    _nextHeartbeat.arm(0s);

    return seastar::make_ready_future<>();
}

} // ns k2
