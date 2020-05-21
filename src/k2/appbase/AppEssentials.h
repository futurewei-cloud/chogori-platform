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

