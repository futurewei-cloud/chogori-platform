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

#include <k2/appbase/Appbase.h>
#include <k2/dto/MessageVerbs.h>

namespace k2::cpo {

void HealthMonitor::_addHBControl(RPCServer&& server, TimePoint nextHB) {
    auto control = seastar::make_lw_shared<HeartbeatControl>();
    control->target = std::move(server);
    control->endpoint = RPC().getTXEndpoint(control->target.ID);
    if (!control->endpoint) {
        K2LOG_W(log::cposvr, "Endpoint is null for URL: {} for role {}", control->target.ID, control->target.role);
        throw std::runtime_error("Endpoint is null for heartbeat target");
    }
    control->nextHeartbeat = nextHB;

    _heartbeats.push_back(std::move(control));
}

void HealthMonitor::_checkHBs() {
    auto now = CachedSteadyClock::now(true);
    Duration nextArm = _interval();

    // We keep one list, _heartbeats, which tracks both time to send the next heartbeat for each target,
    // and the time that the heartbeat will expire for each target
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
                K2LOG_W(log::cposvr, "HB target is dead: {}", **it);
                _downEvents++;
                if ((*it)->target.role == _nodepoolRole) {
                    _nodepoolDown++;
                } else if ((*it)->target.role == _tsoRole) {
                    _TSODown++;
                } else if ((*it)->target.role == _persistRole) {
                    _persistDown++;
                }
            }
        }

        // Send a new heartbeat request to target.
        dto::HeartbeatRequest request{.lastToken = (*it)->lastToken, .interval = _interval(),
                                        .deadThreshold = _deadThreshold()};

        (*it)->heartbeatInFlight = true;
        (*it)->nextHeartbeat = now + _interval();
        // last_it will be captured in the heartbeat request, so that it can be moved to the end
        // when we get a response
        auto last_it = _heartbeats.insert(_heartbeats.end(), *it);
        // This both iterates over the list and sets the current target to the end, since we just
        // changed the nextHeartbeat variable and we need to keep the list sorted
        it = _heartbeats.erase(it);

        k2::OperationLatencyReporter reporter(_heartbeatLatency);
        (*last_it)->heartbeatRequest = (*last_it)->heartbeatRequest
        .then([this, request, hb_ptr=*last_it, last_it, reporter=std::move(reporter)] () mutable {
            return RPC()
            .callRPC<dto::HeartbeatRequest, dto::HeartbeatResponse>(dto::Verbs::CPO_HEARTBEAT, request,
                                                                    *((*last_it)->endpoint), _interval() / 2)
            .then([this, hb_ptr, it=last_it, reporter=std::move(reporter)] (auto&& result) mutable {
                auto& [status, response] = result;
                K2LOG_V(log::cposvr, "Heartbeat response for {}", *hb_ptr);
                reporter.report();

                if (!status.is2xxOK()) {
                    // Missed HB will be counted against the target the next time this target reaches the
                    // head of the list
                    K2LOG_I(log::cposvr, "Bad heartbeat response: {}", status);
                    _heartbeatsLost++;
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
            .handle_exception([this, hb_ptr=*last_it] (auto exc){
                K2LOG_W_EXC(log::cposvr, exc, "caught exception in heartbeat RPC for target {}", *hb_ptr);
                return seastar::make_ready_future();
            });
        });
    }

    _nextHeartbeat.arm(nextArm);
}

seastar::future<> HealthMonitor::gracefulStop() {
    if (seastar::this_shard_id() != _heartbeatMonitorShardId()) {
        return seastar::make_ready_future<>();
    }

    _running = false;
    seastar::future<> timer = _nextHeartbeat.stop();

    return seastar::do_with(std::vector<seastar::future<>>{}, [this, timer=std::move(timer)]
                                                              (std::vector<seastar::future<>>& futs) mutable {
        futs.reserve(_heartbeats.size() + 1);

        futs.push_back(std::move(timer));

        for (auto hb : _heartbeats) {
            futs.push_back(std::move(hb->heartbeatRequest));
        }

        return seastar::when_all_succeed(futs.begin(), futs.end()).discard_result();
    });
}

void HealthMonitor::_registerMetrics() {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;
    labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

    _metric_groups.add_group("HealthMonitor", {
        sm::make_gauge("nodepool_total",[this]{ return _nodepoolEndpoints().size();},
                        sm::description("Number nodepool node targets"), labels),
        sm::make_gauge("tso_total",[this]{ return _TSOEndpoints().size();},
                        sm::description("Number TSO targets"), labels),
        sm::make_gauge("persistence_total",[this]{ return _persistEndpoints().size();},
                        sm::description("Number persistence targets"), labels),
        sm::make_gauge("nodepool_down", _nodepoolDown, sm::description("Number of nodepool nodes considered dead"), labels),
        sm::make_gauge("TSO_down", _TSODown, sm::description("Number of TSO targets considered dead"), labels),
        sm::make_gauge("persistence_down", _persistDown, sm::description("Number of persistence targets considered dead"), labels),
        sm::make_counter("heartbeats_lost", _heartbeatsLost, sm::description("Number of heartbeat lost"), labels),
        sm::make_counter("down_events", _downEvents, sm::description("Number of down events"), labels),
        sm::make_histogram("heartbeat_latency", [this]{ return _heartbeatLatency.getHistogram();},
                sm::description("Latency of heartbeat requests"), labels),
    });
}

// TODO: These three get*Endpoints functions are implemented assuming the auto-rrdma
// protocol will be improved to support full auto-negotiation. If it isn't then one option
// is to change these to return the endpoints received from the RPCServers heartbeat responses.
seastar::future<std::vector<String>> HealthMonitor::getNodepoolEndpoints() {
    return AppBase().getDist<HealthMonitor>().invoke_on(_heartbeatMonitorShardId(),
                            &HealthMonitor::getNodepoolEndpointsHelper);
}

seastar::future<std::vector<String>> HealthMonitor::getTSOEndpoints() {
    return AppBase().getDist<HealthMonitor>().invoke_on(_heartbeatMonitorShardId(),
                            &HealthMonitor::getTSOEndpointsHelper);
}

seastar::future<std::vector<String>> HealthMonitor::getPersistEndpoints() {
    return AppBase().getDist<HealthMonitor>().invoke_on(_heartbeatMonitorShardId(),
                            &HealthMonitor::getPersistEndpointsHelper);
}

std::vector<String> HealthMonitor::getNodepoolEndpointsHelper() {
    return _nodepoolEndpoints();
}

std::vector<String> HealthMonitor::getTSOEndpointsHelper() {
    return _TSOEndpoints();
}

std::vector<String> HealthMonitor::getPersistEndpointsHelper() {
    return _persistEndpoints();
}

seastar::future<> HealthMonitor::start() {
    if (seastar::this_shard_id() != _heartbeatMonitorShardId()) {
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
        server.role = _nodepoolRole;

        _addHBControl(std::move(server), now + delay);

        ++count;
        if (count == _batchSize()) {
            delay += _batchWait();
            count = 0;
        }
    }

    for(const String& ep_url : _TSOEndpoints()) {
        RPCServer server;
        server.ID = ep_url;
        server.role = _tsoRole;

        _addHBControl(std::move(server), now + delay);

        ++count;
        if (count == _batchSize()) {
            delay += _batchWait();
            count = 0;
        }
    }

    for(const String& ep_url : _persistEndpoints()) {
        RPCServer server;
        server.ID = ep_url;
        server.role = _persistRole;

        _addHBControl(std::move(server), now + delay);

        ++count;
        if (count == _batchSize()) {
            delay += _batchWait();
            count = 0;
        }
    }

    _registerMetrics();

    _nextHeartbeat.setCallback([this] {
        K2LOG_D(log::cposvr, "Heartbeat timer fired");
        _checkHBs();
    });
    _nextHeartbeat.arm(0s);

    return seastar::make_ready_future<>();
}

} // ns k2
