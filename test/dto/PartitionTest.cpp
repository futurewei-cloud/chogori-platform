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

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <seastar/core/sleep.hh>

const char* collname = "k23si_test_collection";

// Integration tests for partition operations
// These assume three partitions with range partition schema: ["", c), [c, e), [e, "") 
class PartitionTest {

public:  // application lifespan
    PartitionTest() : _client(k2::K23SIClientConfig()) { K2INFO("ctor");}
    ~PartitionTest(){ K2INFO("dtor");}

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2INFO("start");

        _testFuture = seastar::make_ready_future()
        .then([this] () {
            return _client.start();
        })
        .then([this] {
            K2INFO("Creating test collection...");
            std::vector<k2::String> rangeEnds;
            rangeEnds.push_back(k2::dto::FieldToKeyString<k2::String>("c"));
            rangeEnds.push_back(k2::dto::FieldToKeyString<k2::String>("e"));
            rangeEnds.push_back("");

            return _client.makeCollection(collname, std::move(rangeEnds));
        })
        .then([](auto&& status) {
            K2EXPECT(status.is2xxOK(), true);
        })
        .then([this] { return runScenario01(); })
        .then([this] {
            K2INFO("======= All tests passed ========");
            exitcode = 0;
        })
        .handle_exception([this](auto exc) {
            try {
                std::rethrow_exception(exc);
            } catch (std::exception& e) {
                K2ERROR("======= Test failed with exception [" << e.what() << "] ========");
                exitcode = -1;
            } catch (...) {
                K2ERROR("Test failed with unknown exception");
                exitcode = -1;
            }
        })
        .finally([this] {
            K2INFO("======= Test ended ========");
            seastar::engine().exit(exitcode);
        });

        return seastar::make_ready_future();
    }

private:
    int exitcode = -1;
    seastar::future<> _testFuture = seastar::make_ready_future();

    k2::K23SIClient _client;
    k2::dto::PartitionGetter _pgetter;

    
public: // tests

// get partition for key through range scheme
seastar::future<> runScenario01() {
    K2INFO("runScenario01");

    return seastar::make_ready_future()
    .then([this] {
        K2INFO("get Partition for key with default reverse and exclusiveKey flag");
        
        k2::dto::Key key{.schemaName = "schema", .partitionKey = "a", .rangeKey = ""};
        k2::dto::PartitionGetter::PartitionWithEndpoint& part = _pgetter.getPartitionForKey(key);
        K2INFO("startkey:" << part.partition->startKey << ", endkey:" << part.partition->endKey << ", endpoint:" 
               << *(part.partition->endpoints.begin()) << ", TXEndpoint:" << (*part.preferredEndpoint).getURL());
        //K2EXPECT(part.partition->startKey, "");
    });
}

};

int main(int argc, char** argv) {
    k2::App app("PartitionTest");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<k2::TSO_ClientLib>(0s);
    app.addApplet<PartitionTest>();
    return app.start(argc, argv);
}

