#include "Prometheus.h"
#include "common/Log.h"

#include <seastar/core/prometheus.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/socket_defs.hh>

namespace k2 {

Prometheus::Prometheus() {
    K2DEBUG("Prometheus ctor");
}
seastar::future<>
Prometheus::start(uint16_t port, const char* helpMessage, const char* prefix) {
    K2INFO("starting prometheus on port: " << port);
    seastar::prometheus::config pctx;
    pctx.metric_help=helpMessage;
    pctx.prefix=prefix;

    return _prometheusServer.start("prometheus").then([this, pctx, port]() {
        seastar::prometheus::start(_prometheusServer, pctx);
        return _prometheusServer.listen(seastar::ipv4_addr{port});
    });
}

seastar::future<> Prometheus::stop() {
    K2INFO("Stopping prometheus");
    return  _prometheusServer.stop();
}

}// namespace k2
