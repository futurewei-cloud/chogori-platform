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

#include <k2/appbase/Appbase.h>
#include <k2/infrastructure/APIServer.h>
#include <k2/cpo/service/CPOService.h>
#include <k2/cpo/service/HealthMonitor.h>

int main(int argc, char** argv) {
    k2::App app("CPOService");
    app.addOptions()
        ("nodepool_endpoints", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list (space-delimited) of nodepool endpoints")
        ("tso_endpoints", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list (space-delimited) of TSO endpoints")
        ("persistence_endpoints", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list (space-delimited) of persistence endpoints")
        ("heartbeat_interval", bpo::value<k2::ParseableDuration>(), "The interval the health monitor uses to send heartbeats")
        ("heartbeat_batch_wait", bpo::value<k2::ParseableDuration>(), "How long the heartbeat monitor waits between batches (only for the start-up set of heartbeats")
        ("heartbeat_batch_size", bpo::value<uint32_t>()->default_value(100), "How many heartbeats the monitor sends before waiting (only for the start-up set of heartbeats")
        ("heartbeat_lost_threshold", bpo::value<uint32_t>()->default_value(3), "How many lost heartbeats are required before the monitor considers a target dead")
        ("heartbeat_monitor_shard_id", bpo::value<uint32_t>()->default_value(1), "Which shard the heartbeat monitor should run on")
        ("txn_heartbeat_deadline", bpo::value<k2::ParseableDuration>(), "The interval clients must use to heartbeat active transactions")
        ("assignment_timeout", bpo::value<k2::ParseableDuration>(), "Timeout for K2 partition assignment")
        ("data_dir", bpo::value<k2::String>(), "The directory where we can keep data");
    app.addApplet<k2::CPOService>([]() mutable -> seastar::distributed<k2::CPOService>& {
        return k2::AppBase().getDist<k2::CPOService>();
    });
    app.addApplet<k2::HealthMonitor>();
    app.addApplet<k2::APIServer>();
    return app.start(argc, argv);
}
