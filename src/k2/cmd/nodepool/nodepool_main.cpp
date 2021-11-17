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
#include <k2/assignmentManager/AssignmentManager.h>
#include <k2/collectionMetadataCache/CollectionMetadataCache.h>
#include <k2/infrastructure/APIServer.h>
#include <k2/nodePoolMonitor/NodePoolMonitor.h>
#include <k2/cpo/client/Heartbeat.h>
#include <k2/tso/client/Client.h>

int main(int argc, char** argv) {
    k2::App app("NodePoolService");

    app.addOptions()
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO) endpoint")
        ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff")
        ("k23si_cpo_endpoint", bpo::value<k2::String>(), "the endpoint for k2 CPO service")
        ("k23si_query_pagination_limit", bpo::value<uint32_t>(), "Max records to return in a single query response")
        ("k23si_query_push_limit", bpo::value<uint32_t>(), "Min records in response needed to avoid a push during query processing")
        ("k23si_max_push_count", bpo::value<uint32_t>(), "Max push count in handleRead and handleWrite")
        ("k23si_persistence_endpoints", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A space-delimited list of k2 persistence endpoints, each core will pick one endpoint");

    app.addApplet<k2::cpo::HeartbeatResponder>();
    app.addApplet<k2::APIServer>();
    app.addApplet<k2::tso::TSOClient>();
    app.addApplet<k2::CollectionMetadataCache>();
    app.addApplet<k2::NodePoolMonitor>();
    app.addApplet<k2::AssignmentManager>();

    return app.start(argc, argv);
}
