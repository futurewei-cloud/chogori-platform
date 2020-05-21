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
#include <k2/nodePoolMonitor/NodePoolMonitor.h>
#include <k2/partitionManager/PartitionManager.h>
#include <k2/tso/client_lib/tso_clientlib.h>

int main(int argc, char** argv) {
    k2::App app("NodePoolService");

    app.addOptions()
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO) endpoint")
        ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff")
        ("k23si_cpo_endpoint", bpo::value<k2::String>(), "the endpoint for k2 CPO service")
        ("k23si_persistence_endpoint", bpo::value<k2::String>(), "the endpoint for k2 persistence");

    app.addApplet<k2::TSO_ClientLib>(10ms);
    app.addApplet<k2::CollectionMetadataCache>();
    app.addApplet<k2::NodePoolMonitor>();
    app.addApplet<k2::AssignmentManager>();
    app.addApplet<k2::PartitionManager>();

    return app.start(argc, argv);
}
