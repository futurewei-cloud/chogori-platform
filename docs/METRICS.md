# Introduction
Chogori platform uses Prometheus style metrics via a built-in prometheus endpoint. There are 3 types of metrics which should cover all use cases:
1. Gauge: this metric is used to report usage levels (e.g. memory_used) - numbers which do not increase indefinitely
2. Count: this metric is used to report numbers which do increase indefinitely (e.g. request_count)
3. Histogram: this is a statistical metric, mostly used for latency reporting. The range of values has to be expected up-front so that the histogram buckets can be set accordingly. It offers very fast client reporting, with cumulative stats computed by the prometheus server.

# Usage
## setup
- create a K2 app using AppBase (see demos and other apps in src/cmd/* ). This ensures that the prometheus endpoint is configured and hooked into your app. The name that you give to your app (in the k2::App() constructor) will be used to report the metrics from your app.
- During the initialization process, create metrics group(s) in order to register your metrics for collection, e.g.
```c++
sm::metric_groups _metric_groups;
```
- Metrics are pre-configured at this point, with labels added up front. Here is an example of adding one of each type of
```c++
// histogram that can be used to report latencies
ExponentialHistogram _requestLatency;


std::vector<sm::label_instance> labels;
// label all metrics with the host on which the app runs. This label can be used to filter/aggregate in dashboards
labels.push_back(sm::label_instance("host", my_hostname));

// create a metrics group
_metric_groups.add_group("session", {
    // register some metrics
    // Gauge registration, reporting a variable directly.
    sm::make_gauge("ack_batch_size", _session.config.ackCount, sm::description("How many messages we ack at once"), labels),
    // counter reporting via lambda. This lambda gets called each time someone visits /metrics (e.g. the prom server)
    sm::make_counter("total_count", [this]{ return _session.getTotalCount();}, sm::description("Total number of requests"), labels),
    // total bytes processed are best emitted via counter
    sm::make_counter("total_bytes", _session.totalSize, sm::description("Total data bytes sent"), labels),

    // latency reporting via lambda which returns a histogram from our ExponentialHistogram
    sm::make_histogram("request_latency", [this]{ return _requestLatency.getHistogram();},
            sm::description("Latency of acks"), labels)
});
```
## reporting
For counters and gauges, the reporting is pretty much handled automatically. For the example above, all that is needed is that your code increments _session.config.ackCount when applicable. The framework will take care of exporting that value to the /metrics endpoint.

For histograms, you have to use the API on ExponentialHistogram to report the latency. The api is simply to report how long something took via standard clock duration:
```c++
auto start = clock::now();
callMethod();
auto duration = clock::now() - start;
_requestLatency.add(duration);
```

The framework will take care of the rest.

## observing

### local
The raw metrics can be observed by pointing your browser to your process. Note the prometheus_port you specified when starting your app, and point your browser to http://<host_where_your_app_runs>:<prometheus_port>/metrics. This will give you the raw metrics from your live app. As can be expected, these can only be reached if your app is still running.

### prometheus/grafana
You can point standard prometheus server to the above port and use Grafana to plot the metrics.
