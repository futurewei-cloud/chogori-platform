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

#include "Prometheus.h"
#include "Util.h"
#include "VirtualNetworkStack.h"
#include <k2/logging/Log.h>

#include <seastar/core/prometheus.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include "Log.h"

namespace k2 {

Prometheus::Prometheus() {
    K2LOG_D(log::prom, "Prometheus ctor");
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
Prometheus::_pullMetrics() {
    auto req = std::make_unique<seastar::httpd::request>();
    auto rep = std::make_unique<seastar::httpd::reply>();
    auto handler = _prometheusServer.server().local()._routes.get_handler(seastar::operation_type::GET, "/metrics", req->param);

    return handler->handle("", std::move(req), std::move(rep))
        .then([] (auto&& rep) {
            rep->set_version("1.1").done();
            return std::move(rep);
        });
}

seastar::future<>
Prometheus::_pushMetricsLoop(seastar::input_stream<char>& input, seastar::output_stream<char>& output, String& hostname) {
    static String responseLine;
    responseLine = "POST " + _pushPath + " HTTP/1.1\r\n";
    return seastar::do_until(
        [&] { return _shouldExit || input.eof(); },
        [&] {
            auto elapsed = Clock::now() - _lastPushTime;
            K2LOG_D(log::prom, "time since last push: {}", elapsed);
            if (elapsed < _config.pushInterval) {
                return seastar::sleep(std::min(Duration(1s), _config.pushInterval - elapsed));
            }
            K2LOG_D(log::prom, "posting data for metrics push");
            return output.write(responseLine)
                .then([&] {
                    return _pullMetrics();
                })
                .then([&](auto&& reply) {
                    reply->add_header("Host", hostname);
                    reply->add_header("Connection", "Keep-Alive");
                    return reply->write_reply_body_to_stream(output);
                })
                .then([&] {
                    return output.write("0\r\n\r\n", 5);
                })
                .then([&] {
                    return output.flush();
                })
                .then([&] {
                    K2LOG_D(log::prom, "Wrote metrics. Awaiting reply...");
                    return input.read();
                })
                .then([&](Binary&& packet) {
                    if (packet.empty()) {
                        K2LOG_W(log::prom, "remote end closed connection");
                        return seastar::make_ready_future();
                    }
                    String resp(packet.get(), packet.size());
                    K2LOG_D(log::prom, "received response to push: {}", resp);
                    if (resp.find("200 OK") == String::npos) {
                        K2LOG_W(log::prom, "remote end returned error response: {}", resp);
                    }
                    _lastPushTime = Clock::now();
                    return seastar::make_ready_future();
                });
        });
}

seastar::future<>
Prometheus::_setupPusher(){
    // we don't have a proper http client library, so we're doing this POST by hand
    return seastar::do_until(
        [this] { return _shouldExit; },
        [this] {
            auto hostname = getHostName();
            K2LOG_D(log::prom, "connecting to push address {}, from hostname {}", _config.pushAddress, hostname);
            auto address = seastar::make_ipv4_address({_config.pushAddress});

            // we don't have a proper http client library, so we're doing this POST by hand
            return seastar::engine().net().connect(std::move(address), {})
                .then([this, hostname=std::move(hostname)] (auto&& fd) {
                    K2LOG_D(log::prom, "connected to push address {}", _config.pushAddress);
                    // inner loop: we have a valid connection, keep trying to send metrics over it
                    return seastar::do_with(std::move(fd), fd.input(), fd.output(), std::move(hostname),
                        [this] (auto& fd, auto& input, auto& output, auto& hostname) {
                            return _pushMetricsLoop(input, output, hostname)
                                .finally([&] {
                                    K2LOG_D(log::prom, "Push loop terminated. Closing socket");
                                    return input.close()
                                        .then_wrapped([this, &fd, &output](auto&& fut) {
                                            K2LOG_D(log::prom, "input close completed");
                                            fut.ignore_ready_future();
                                            try {fd.shutdown_input();}catch(...){}
                                            return output.close();
                                        })
                                        .then_wrapped([this, &fd](auto&& fut){
                                            K2LOG_D(log::prom, "output close completed");
                                            fut.ignore_ready_future();
                                            try {
                                                fd.shutdown_output();
                                            } catch (...) {
                                            }
                                            return seastar::make_ready_future();
                                        });
                                });
                    });
            })
            .handle_exception([](auto exc) {
                K2LOG_W_EXC(log::prom, exc, "Exception on connect prometheus push.");
                return seastar::sleep(1s);
            });
        });
}

seastar::future<> Prometheus::start(PromConfig cfg) {
    _config = std::move(cfg);
    K2LOG_I(log::prom, "starting prometheus on port={}, msg={}, prefix={}", _config.port, _config.helpMessage, _config.prefix);

    if (!_config.pushAddress.empty() && seastar::this_shard_id() == 0) {
        _config.pushAddress = String(_config.pushAddress);
        _pushPath = String("/metrics/job/k2/instance/") + getHostName() + ":" + std::to_string(::getpid());
        K2LOG_I(log::prom, "enabling prometheus push to url: {}{}, with interval: {}", _config.pushAddress, _pushPath, _config.pushInterval);

        // outer loop: create a connection and keep pushing metrics. Keep creating a new connection if a connection
        // failure occurs.
        _pusher = _setupPusher();
    }

    seastar::prometheus::config pctx;
    pctx.metric_help=_config.helpMessage.c_str();
    pctx.prefix=_config.prefix.c_str();

    return _prometheusServer.start("prometheus").then([this, pctx]() {
        return seastar::prometheus::start(_prometheusServer, pctx).then([this]{
            return _prometheusServer.listen(seastar::ipv4_addr{_config.port});
        });
    });
}

seastar::future<> Prometheus::stop() {
    K2LOG_I(log::prom, "Stopping prometheus");
    _shouldExit = true;
    return _pusher.then([this] {
        return _prometheusServer.stop();
    });
}

ExponentialHistogram::ExponentialHistogram(uint64_t start, uint64_t end, double rate) : _rate(rate), _lograte(std::log(rate)) {
    // for a sequence starting at `start`, ending at `end` with rate of change `rate`, we want to find
    // the first value K for which
    // start*rate^K > end -->
    // rate^K > end/start -->
    // K > logbase(val=end/start, base=rate) -->
    // K > ln(end/start)/ln(rate)
    K2ASSERT(log::prom, start >= 1, "invalid start");
    uint64_t numbuckets = uint64_t(std::log(double(end) / start) / _lograte) + 1;
    K2ASSERT(log::prom, numbuckets <= MAX_NUM_BUCKETS, "invalid number of buckets");
    _histogram.buckets.resize(numbuckets);
    // we have to assign an upper bound for each bucket.
    for (size_t i = 0; i < _histogram.buckets.size(); ++i) {
        _histogram.buckets[i].upper_bound = start * std::pow(rate, i);
    }
    _promHistogram = _histogram;
}

seastar::metrics::histogram& ExponentialHistogram::getHistogram() {
    uint64_t total = 0;
    // go through the non-cumulative histogram, and update the promHistogram with cumulative count
    for (size_t i = 0; i < _promHistogram.buckets.size(); ++i) {
        total += _histogram.buckets[i].count;
        _promHistogram.buckets[i].count = total;
    }
    _promHistogram.sample_count = _histogram.sample_count;
    _promHistogram.sample_sum = _histogram.sample_sum;
    return _promHistogram;
}

void ExponentialHistogram::add(double sample) {
    K2ASSERT(log::prom, sample >= 0, "invalid sample");
    uint64_t bucket = 0;
    if (sample > 1) {
        bucket = std::min(uint64_t(std::log(sample) / _lograte), _histogram.buckets.size() - 1);
    }
    _histogram.buckets[bucket].count += 1;  // bucket count
    _histogram.sample_count += 1;           // global count
    _histogram.sample_sum += sample;        // global sum
}
}// namespace k2
