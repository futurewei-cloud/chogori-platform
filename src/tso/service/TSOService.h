#pragma once

// stl
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <string>

// third-party
#include <seastar/core/app-template.hh>  // for app_template
#include <seastar/core/distributed.hh>   // for distributed<>
#include <seastar/core/future.hh>        // for future stuff
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>  // metrics
#include <seastar/core/reactor.hh>               // for app_template
#include <seastar/core/timer.hh>                 // periodic timer
#include <seastar/util/reference_wrapper.hh>     // for app_template

#include <transport/RPCDispatcher.h>

using namespace std::chrono_literals;  // so that we can type "1ms"
namespace bpo = boost::program_options;
namespace sm = seastar::metrics;

namespace k2 {
class TSOService {
    public: // public types
    // distributed version of the class
    typedef seastar::distributed<TSOService> Dist_t;

    public: // application lifespan
    TSOService(k2::RPCDispatcher::Dist_t& dispatcher, const bpo::variables_map& config);
    ~TSOService();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();

    private: // private members
    k2::RPCDispatcher& _disp;
    bool _stopped;
};  // class TSOService

} // namespace k2
