// This is a convenience header, which helps get access to various concerns for k2 applications
// k2 stuff
#include <k2/common/Log.h>               // for access to initialized logging
#include <k2/config/Config.h>            // for access to initialized Config
#include <k2/transport/Prometheus.h>     // for access to initialized etrics
#include <k2/transport/RPCDispatcher.h>  // for access to initialized RPC
#include <k2/common/Common.h>            // for to common data types

// seastar stuff
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>  // metrics
#include <seastar/core/timer.hh>                 // periodic timer
#include <seastar/util/reference_wrapper.hh>     // for passing references around

using namespace std::chrono_literals; // so that we can type "1ms"
namespace bpo = boost::program_options;
namespace sm = seastar::metrics;

