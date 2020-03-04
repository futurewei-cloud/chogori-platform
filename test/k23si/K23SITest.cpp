#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <seastar/core/sleep.hh>

#include <k2/dto/K23SI.h>
#include <k2/dto/Collection.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>

namespace k2 {
const char* collname = "k23si_test_collection";

class K23SITest {

public:  // application lifespan
    K23SITest() { K2INFO("ctor");}
    ~K23SITest(){ K2INFO("dtor");}

    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2INFO("stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        K2INFO("start");

        K2EXPECT(_k2ConfigEps().size(), 3);
        for (auto& ep: _k2ConfigEps()) {
            _k2Endpoints.push_back(RPC().getTXEndpoint(ep));
        }

        _cpoEndpoint = RPC().getTXEndpoint(_cpoConfigEp());

        // let start() finish and then run the tests
        _testFuture = seastar::sleep(1ms)
            .then([this] {
                K2INFO("Creating test collection...");
                auto request = dto::CollectionCreateRequest{
                    .metadata{
                        .name = collname,
                        .hashScheme = dto::HashScheme::HashCRC32C,
                        .storageDriver = dto::StorageDriver::K23SI,
                        .capacity{
                            .dataCapacityMegaBytes = 1000,
                            .readIOPs = 100000,
                            .writeIOPs = 100000
                        },
                        .retentionPeriod = Duration(1h)*90*24
                    },
                    .clusterEndpoints = _k2ConfigEps()
                };
                return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>
                        (dto::Verbs::CPO_COLLECTION_CREATE, std::move(request), *_cpoEndpoint, 1s);
            })
            .then([](auto response) {
                // response for collection create
                auto& [status, resp] = response;
                K2EXPECT(status, Status::S201_Created());
                // wait for collection to get assigned
                return seastar::sleep(100ms);
            })
            .then([this] {
                // check to make sure the collection is assigned
                auto request = dto::CollectionGetRequest{.name = collname};
                return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>
                    (dto::Verbs::CPO_COLLECTION_GET, std::move(request), *_cpoEndpoint, 100ms);
            })
            .then([this](auto response) {
                // check collection was assigned
                auto& [status, resp] = response;
                K2EXPECT(status, Status::S200_OK());
                _pgetter = dto::PartitionGetter(std::move(resp.collection));
            })
            .then([this] { return runScenario01(); })
            .then([this] { return runScenario02(); })
            .then([this] { return runScenario03(); })
            .then([this] { return runScenario04(); })
            .then([this] { return runScenario05(); })
            .then([] {
                K2INFO("======= All tests passed ========");
            })
            .handle_exception([this](auto exc) {
                try {
                    std::rethrow_exception(exc);
                } catch (RPCDispatcher::RequestTimeoutException& exc) {
                    K2ERROR("======= Test failed due to timeout ========");
                    exitcode = -1;
                } catch (std::exception& e) {
                    K2ERROR("======= Test failed with exception [" << e.what() << "] ========");
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
    int exitcode;
    ConfigVar<std::vector<String>> _k2ConfigEps{"k2_endpoints"};
    ConfigVar<String> _cpoConfigEp{"cpo_endpoint"};

    std::vector<std::unique_ptr<k2::TXEndpoint>> _k2Endpoints;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;

    seastar::future<> _testFuture = seastar::make_ready_future();

    dto::PartitionGetter _pgetter;
    uint64_t txnids = 10000;
public: // tests

seastar::future<> runScenario01() {
    K2INFO("Scenario 01: empty node");
    return seastar::make_ready_future()
    .then([this] {
        dto::Partition* part = _pgetter.getPartitionForKey(dto::Key{.partitionKey="Key1", .rangeKey="rKey1"});
        // read wrong collection
        dto::K23SIReadRequest request {
            .pvid = part->pvid,
            .collectionName = "somebadcoll",
            .mtr {
                .txnid = txnids++,
                .timestamp = dto::Timestamp(100000, 1, 1000),
                .priority = dto::TxnPriority::Medium
            },
            .key{
                .partitionKey = "key1",
                .rangeKey = "rkey1"
            }
        };
        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse<Payload>>
            (dto::Verbs::K23SI_READ, std::move(request), *_k2Endpoints[0], 100ms)
        .then([](auto response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Status::S410_Gone());
        });
    });
    /*
    Scenario 1: empty node:

        - read wrong partition
            expect 410 Gone
        - read out-of-date partition version
            expect 410 Gone
        - read empty key wrong partition
            expect 410 Gone
        - read empty key out-of-date partition version
            expect 410 Gone
        - read with empty key
            expect 404 not found
        - read with only partitionKey
            expect 404 not found
        - read with partition and range key
            expect 404 not found
    */
}
seastar::future<> runScenario02() {
    K2INFO("Scenario 02");
    /*
    Scenario 2: node with single version data:
            - ("pkey1","", v10) -> commited
            - ("pkey2","", v11) -> WI
            - ("pkey3","", v12) -> aborted but not cleaned
            - ("pkey3","", v13) -> aborted but not cleaned
cases requiring client to refresh collection pmap
        - read all valid keys; wrong collection, wrong partition
            expect 410 Gone
        - read invalid key; wrong collection, wrong partition
            expect 410 Gone
        - read all valid keys; wrong collection, out-of-date partition
            expect 410 Gone
        - read invalid key; wrong collection, out-of-date partition
            expect 410 Gone
        - read all valid keys; correct collection, wrong partition
            expect 410 Gone
        - read invalid key; correct collection, wrong partition
            expect 410 Gone
        - read all valid keys; correct collection, out-of-date partition version
            expect 410 Gone
        - read invalid key; correct collection, out-of-date partition version
            expect 410 Gone
        - read empty key wrong partition
            expect 410 Gone
        - read empty key out-of-date partition version
            expect 410 Gone

        - read ("pkey1", "", v10)
            expect 200 OK with data
        - read ("pkey1", "", v11)
            expect 200 OK with data
        - read ("pkey1", "", v9)
            expect 404 not found

        - read ("pkey1", "", v10)
            expect 200 OK with data
        - read ("pkey1", "", v11)
            expect 200 OK with data
        - read ("pkey1", "", v9)
            expect 404 not found

    */
    return seastar::make_ready_future();
}
seastar::future<> runScenario03() {
    K2INFO("Scenario 03");
    return seastar::make_ready_future();
}
seastar::future<> runScenario04() {
    K2INFO("Scenario 04");
    return seastar::make_ready_future();
}
seastar::future<> runScenario05() {
    K2INFO("Scenario 05");
    return seastar::make_ready_future();
}

}; // class K23SITest
} // ns k2

int main(int argc, char** argv) {
    k2::App app;
    app.addOptions()("k2_endpoints", bpo::value<std::vector<std::string>>()->multitoken(), "The endpoints of the k2 cluster");
    app.addOptions()("cpo_endpoint", bpo::value<std::string>(), "The endpoint of the CPO");
    app.addApplet<k2::K23SITest>();
    return app.start(argc, argv);
}
