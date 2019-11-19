//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <cmath>
#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/metrics_types.hh>
#include <seastar/http/httpd.hh>


/*
see https://prometheus.io/docs/concepts/metric_types/ for reference on metric types

label_instance = (string K, string V)
T&&val can either be a lambda which returns the current value, or a reference to a member which holds
the current value, e.g. the following two metrics are identical:

sm::make_counter("total_count", _session.totalCount, sm::description("Total number of requests"), labels)
sm::make_counter("total_count", [&] {return _session.totalCount;}, sm::description("Total number of requests"), labels)

// Gauge. Use to report values which go up/down (e.g. free memory, CPU utilization, temperature)
make_gauge(string name, T&& val, string description, std::vector<label_instance> labels)
// Counter. Use to report increasing values (e.g. request count, number of allocations)
make_counter(string name, T&& val, string description, std::vector<label_instance> labels)
// histograms. Use to report latencies. Percentile calculations are done server-side with interpolation
make_histogram(string name, T&& val, string description, std::vector<label_instance> labels)


Seastar also provides these utility methods, I think left over from collectd integration:

// Derive. Supposed to be used for metrics which should be looked at as derived (e.g. rate of request rate increase)
// It really is reported as a Gauge and you have to add some function math in your Grafana dashboard to calculate rate
// It just adds a label "type"="derive"
make_derive(string name, string description, std::vector<label_instance> labels, Func&& func)

// Reports total(e.g. network total) and current(e.g. free memory) bytes
// It is really a counter and gauge, each labeled with a "type" label of "total_bytes" or "current_bytes"
make_total_bytes(string name, Func&& func, string description, std::vector<label_instance> labels)
make_current_bytes(string name, Func&& func, string description, std::vector<label_instance> labels)

// to report queue length
// Just a gauge, labeled with "type"="queue_length"
make_queue_length(string name, Func&& func, string description, std::vector<label_instance> labels)

// to report operation count (counter, labeled with type="total_operations")
make_total_operations(string name, Func&& func, string description, std::vector<label_instance> labels)

*/

namespace k2 {

// This class is used to initialize and manage(start/stop) the metrics subsystem.
class Prometheus {
public:
    // ctor
    Prometheus();

    // initialize prometheus.
    // @param port: the http port on which we will expose metrics
    // @param helpMessage: message that describes what these metrics are about (e.g. "K2 NodePool metrics")
    // @param prefix: this string will be prefix of the exported Prometheus metrics. It is used to key dashboards
    seastar::future<> start(uint16_t port, const char* helpMessage, const char* prefix);

    // this method should be called to stop the server (usually on engine exit)
    seastar::future<> stop();

private:
    seastar::httpd::http_server_control _prometheusServer;

}; // class prometheus

uint64_t constexpr MAX_NUM_BUCKETS = 1000; // make sure users aren't accidentally creating too-large histograms

// This histogram creates buckets which exponentially grow/shrink, depending on the given rate
// It is normally used to report latencies, or any other positive samples
class ExponentialHistogram {
public:
    // Create a new histogram
    // The defaults are useful for reporting latencies in usecs from 1usec to 10sec (89 buckets):
    // 1.00 1.20 1.44 1.73 2.07 2.49 2.99 3.58 4.30 5.16 6.19 7.43 8.92 10.70 12.84 15.41 18.49 22.19 26.62
    // 31.95 38.34 46.01 55.21 66.25 79.50 95.40 114.48 137.37 164.84 197.81 237.38 284.85 341.82 410.19 492.22 590.67
    // 708.80 850.56 1020.67 1224.81 1469.77 1763.73 2116.47 2539.77 3047.72 3657.26 4388.71 5266.46 6319.75 7583.70
    // 9100.44 10920.53 13104.63 15725.56 18870.67 22644.80 27173.76 32608.52 39130.22 46956.26 56347.51 67617.02
    // 81140.42 97368.50 116842.21 140210.65 168252.78 201903.33 242284.00 290740.80 348888.96 418666.75 502400.10
    // 602880.12 723456.14 868147.37 1041776.84 1250132.21 1500158.65 1800190.39 2160228.46 2592274.15 3110728.99
    // 3732874.78 4479449.74 5375339.69 6450407.62 7740489.15 9288586.98 11146304.37
    // @param start. The value limit for the first bucket. All reported values <= start will end up here. Must be >=1
    // @param end. The value limit for the last bucket. All reported values >=end will end up here.
    // @param rate. The exponential rate at which we allocate buckets from start..end. It dictates how many buckets will
    // get created.
    ExponentialHistogram(uint64_t start=1, uint64_t end=10'000'000, double rate=1.2);

    // this is the raw histogram (vector<bucket>) which we need to provide to the metrics subsystem for reporting
    seastar::metrics::histogram& getHistogram();

    // report a new sample. The sample must be >=0
    void add(double sample);

    // Convenience method we can use to record time durations.
    // The durations are recorded with microsecond resolution by default.
    template<typename Resolution=std::micro>
    void add(std::chrono::steady_clock::duration sample) {
        // convert to double microseconds
        std::chrono::duration<double, Resolution> sample_usecs = sample;
        add(sample_usecs.count());
    }

private:
    double _rate;
    double _lograte;
    // this histogram is used to keep the current counters in a non-cumulative way for O(1) ops
    seastar::metrics::histogram _histogram;

    // Prometheus expects a cumulative histogram. So we compute one every time we're asked to scrape
    seastar::metrics::histogram _promHistogram;

}; // class ExponentialHistogram
} // k2 namespace
