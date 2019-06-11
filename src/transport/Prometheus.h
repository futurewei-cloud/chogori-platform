//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>

namespace k2 {

class Prometheus {
public:
// ctor
Prometheus();

// initialize prometheus.
// @param port: the http port on which we will expose metrics
// @param prefix: this string will be prefix of the exported Prometheus metrics. It is used to key dashboards
// @param helpMessage: message that describes what these metrics are about (e.g. "K2 NodePool metrics")
seastar::future<> Start(uint16_t port, const char* helpMessage, const char* prefix);

// this method should be called to stop the server (usually on engine exit)
seastar::future<> Stop();

private:
    seastar::httpd::http_server_control _prometheusServer;

}; // class prometheus

} // k2 namespace
