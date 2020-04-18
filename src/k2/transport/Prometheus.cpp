#include "Prometheus.h"
#include <k2/common/Log.h>

#include <seastar/core/prometheus.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/socket_defs.hh>

namespace k2 {

Prometheus::Prometheus() {
    K2DEBUG("Prometheus ctor");
}
seastar::future<>
Prometheus::start(uint16_t port, const char* helpMessage, const char* prefix) {
    K2INFO("starting prometheus on port: " << port << ", msg=" << helpMessage <<", prefix=" << prefix);
    seastar::prometheus::config pctx;
    pctx.metric_help=helpMessage;
    pctx.prefix=prefix;

    return _prometheusServer.start("prometheus").then([this, pctx, port]() {
        return seastar::prometheus::start(_prometheusServer, pctx).then([this, port]{
            return _prometheusServer.listen(seastar::ipv4_addr{port});
        });
    });
}

seastar::future<> Prometheus::stop() {
    K2INFO("Stopping prometheus");
    return  _prometheusServer.stop();
}

ExponentialHistogram::ExponentialHistogram(uint64_t start, uint64_t end, double rate) : _rate(rate), _lograte(std::log(rate)) {
    // for a sequence starting at `start`, ending at `end` with rate of change `rate`, we want to find
    // the first value K for which
    // start*rate^K > end -->
    // rate^K > end/start -->
    // K > logbase(val=end/start, base=rate) -->
    // K > ln(end/start)/ln(rate)
    assert(start >= 1);
    uint64_t numbuckets = uint64_t(std::log(double(end) / start) / _lograte) + 1;
    assert(numbuckets <= MAX_NUM_BUCKETS);
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
    assert(sample >= 0);
    uint64_t bucket = 0;
    if (sample > 1) {
        bucket = std::min(uint64_t(std::log(sample) / _lograte), _histogram.buckets.size() - 1);
    }
    _histogram.buckets[bucket].count += 1;  // bucket count
    _histogram.sample_count += 1;           // global count
    _histogram.sample_sum += sample;        // global sum
}
}// namespace k2
