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

#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>

#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>
#include <k2/dto/Collection.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/module/k23si/Module.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC


namespace k2{
const char* collname = "3si_txn_collection";
const dto::HashScheme hashScheme = dto::HashScheme::HashCRC32C; 

struct DataRec {
    String f1;
    String f2;
    K2_PAYLOAD_FIELDS(f1, f2);
    bool operator==(const DataRec& o) {
        return f1 == o.f1 && f2 == o.f2;
    }
    friend std::ostream& operator<<(std::ostream& os, const DataRec& r) {
        return os << "{f1=" << r.f1 << ", f2=" << r.f2 << "}";
    }
};


class txn_testing {

public:		// application
	txn_testing() { K2INFO("ctor"); }
	~txn_testing(){ K2INFO("dtor"); }

	static seastar::future<dto::Timestamp> getTimeNow() {
        // TODO call TSO service with timeout and retry logic
        auto nsecsSinceEpoch = sys_now_nsec_count();
        return seastar::make_ready_future<dto::Timestamp>(dto::Timestamp(nsecsSinceEpoch, 123, 1000));
    }

	// required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        return std::move(_testFuture);
    }

	seastar::future<> start(){
        K2INFO("start txn_testing..");

        K2EXPECT(_k2ConfigEps().size(), 3);
        for (auto& ep: _k2ConfigEps()) {
            _k2Endpoints.push_back(RPC().getTXEndpoint(ep));
        }

        _cpo_client = CPOClient(_cpoConfigEp());
        _cpoEndpoint = RPC().getTXEndpoint(_cpoConfigEp());
        _testTimer.set_callback([this] {
            _testFuture = testScenario00()
            //.then([this] { return testScenario00(); })
            //.then([this] { return testScenario02(); })
            .then([this] {
                K2INFO("======= All tests passed ========");
                exitcode = 0;
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
        });

        _testTimer.arm(0ms);
        return seastar::make_ready_future();
    }


private:
	int exitcode = -1;
	seastar::future<> _testFuture = seastar::make_ready_future();
	ConfigVar<std::vector<String>> _k2ConfigEps{"k2_endpoints"};
	ConfigVar<String> _cpoConfigEp{"cpo_endpoint"};
	CPOClient _cpo_client;
	seastar::timer<> _testTimer;
	
	std::vector<std::unique_ptr<k2::TXEndpoint>> _k2Endpoints;
	std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;

	uint64_t txnids = 1029;

public:		// test	scenario

// Any request (READ, WRITE, PUSH, END, FINALIZE, HEARTBEAT) should observe a timeout(404_not_found)
// example of command: CPO_COLLECTION_GET & K23SI_WRITE
seastar::future<> testScenario00() {
	K2INFO("+++++++ TestScenario 00: unassigned nodes +++++++");
	K2INFO("--->Test SETUP: start a cluster but don't create a collection. Any requests observe a timeout.");
	
	return seastar::make_ready_future()	
	.then([this] {
		// command: K23SI_WRITE
		K2INFO("Test case SC00_1: K23SI_WRITE");
		return getTimeNow()
		.then([&](dto::Timestamp&& ts) {
			return seastar::do_with(
				dto::K23SI_MTR{
					.txnid = txnids++,
					.timestamp = std::move(ts),
					.priority = dto::TxnPriority::Medium
				},
				dto::Key{.partitionKey = "SC00_pKey1", .rangeKey = "SC00_rKey1"},
				dto::Key{.partitionKey = "SC00_pKey1", .rangeKey = "SC00_rKey1"},
				DataRec{.f1="field1", .f2="field2"},
				[this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec){
					dto::K23SIWriteRequest<DataRec> request;
                    request.collectionName = collname;
                    request.mtr = mtr;
                    request.trh = trh;
                    request.isDelete = false;
                    request.designateTRH = true;
                    request.key = key;
                    request.value.val = rec;
                    return RPC().callRPC<dto::K23SIWriteRequest<DataRec>, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, request, *_k2Endpoints[0], 100ms)
                    .then([this](auto&& response) {
                		// response: K23SI_WRITE
                		auto& [status, resp] = response;
                        K2EXPECT(status, Statuses::S503_Service_Unavailable);
                		K2INFO("response: K23SI_WRITE. " << "status: " << status.code << " with error MESG: " << status.message);
                	});
				}
			);
		});	
	}) // end K23SI_WRITE
    .then([this] {
        // command: K23SI_READ     
        K2INFO("Test case SC00_2: K23SI_READ");
        dto::Partition::PVID pvid0;
        dto::K23SIReadRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .mtr = {txnids++, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .key = {"SC00_pKey1", "SC00_rKey1"}
        };
        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse<Payload>>
                (dto::Verbs::K23SI_READ, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_READ. " << "status: " << status.code << " with error MESG: " << status.message);
        });
    }) // end  K23SI_READ 
    .then([this] {
        // command: K23SI_TXN_PUSH
        K2INFO("Test case SC00_3: K23SI_TXN_PUSH");        
        dto::Partition::PVID pvid0;
        dto::K23SITxnPushRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .key = {"SC00_pKey1", "SC00_rKey1"},
            .incumbentMTR = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .challengerMTR = {txnids-2, dto::Timestamp(20200101, 1, 1000), dto::TxnPriority::Medium}
        };
        return RPC().callRPC<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
                (dto::Verbs::K23SI_TXN_PUSH, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_PUSH. " << "status: " << status.code << " with error MESG: " << status.message);
        });
    }) // end K23SI_TXN_PUSH
    .then([this] {
        // command: K23SI_TXN_END
        K2INFO("Test case SC00_4: K23SI_TXN_END");        
        dto::Partition::PVID pvid0;
        dto::K23SITxnEndRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .key = {"SC00_pKey1", "SC00_rKey1"},
            .mtr = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .action = dto::EndAction::Abort,
            .writeKeys = {{"SC00_pKey1", "SC00_rKey1"}},
            .syncFinalize = false
        };
        return RPC().callRPC<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
                (dto::Verbs::K23SI_TXN_END, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_END. " << "status: " << status.code << " with error MESG: " << status.message);
        });
    }) // end K23SI_TXN_END
    .then([this] {
        // command: K23SI_TXN_FINALIZE
        K2INFO("Test case SC00_5: K23SI_TXN_FINALIZE");        
        dto::Partition::PVID pvid0;
        dto::K23SITxnFinalizeRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .trh = {"SC00_pKey1", "SC00_rKey1"},
            .mtr = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
            .key = {"SC00_pKey1", "SC00_rKey1"},
            .action = dto::EndAction::Abort
        };
        return RPC().callRPC<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
                (dto::Verbs::K23SI_TXN_FINALIZE, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_FINALIZE. " << "status: " << status.code << " with error MESG: " << status.message);
        });       
    }) // end K23SI_TXN_FINALIZE
    .then([this] {
        // command: K23SI_TXN_HEARTBEAT
        K2INFO("Test case SC00_6: K23SI_TXN_HEARTBEAT");        
        dto::Partition::PVID pvid0;
        dto::K23SITxnHeartbeatRequest request {
            .pvid = pvid0,
            .collectionName = collname,
            .key = {"SC00_pKey1", "SC00_rKey1"},
            .mtr = {txnids-1, dto::Timestamp(20200828, 1, 1000), dto::TxnPriority::Medium},
        };
        return RPC().callRPC<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
                (dto::Verbs::K23SI_TXN_HEARTBEAT, request, *_k2Endpoints[0], 100ms)
        .then([](auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(status, Statuses::S503_Service_Unavailable);
            K2INFO("response: K23SI_TXN_HEARTBEAT. " << "status: " << status.code << " with error MESG: " << status.message);
        });       
        
    });
}


};	// class k23si_testing
}	// ns k2

int main(int argc, char** argv){
	k2::App app("txn_testing");
	app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
	app.addApplet<k2::txn_testing>();
	return app.start(argc, argv);
}

