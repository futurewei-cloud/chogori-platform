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
            //.then([this] { return testScenario01(); })
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

	dto::PartitionGetter _pgetter;
	uint64_t txnids = 1029;

	seastar::future<int> checkCollection(const String& cname){
		// check whether cname collection is exist
		auto request = dto::CollectionGetRequest{.name = cname};
		auto str = _cpoEndpoint->getURL();
		K2INFO("f_checkCollection, CPO_endpoint:" << str << " CollName:" << cname);
		return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 300ms)
	    .then([cname](auto&& response) {
	    	// command: CPO_COLLECTION_GET 
	        auto& [status, resp] = response;
			std::cout << "status.code: " << status.code << ", status.msg: " << status.message << ".\n";
			std::cout << "resp.collection.metadata.name:\"" << resp.collection.metadata.name << "\".\n";
			if(status.code != Statuses::S200_OK.code){
				K2INFO("collection \"" << cname << "\" is not exist. doWrite exit(1) with error CODE: "
					<< status.code << " with error MESG: " << status.message);
			}
			else{
				K2INFO("")
			}
			K2INFO("status code:" << status.code);
			return status.code;
	    });
	}

	template <typename DataType>
    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    doWrite(const dto::Key& key, const DataType& data, const dto::K23SI_MTR& mtr, const dto::Key& trh, const String& cname, bool isDelete, bool isTRH) {			
    	K2INFO("FIRST: do_write() check WHETHER the dest collection is exist?");
		return checkCollection(collname)
			.then([&](auto&& retCode) {
				// response: checkCollection
				K2INFO("doWrite::checkCollection::retCode:" << retCode);
				if(retCode == Statuses::S200_OK.code){
					K2INFO("doWrite::checkCollection success. DO_WRITE with original kv-interface, key=" << key << ",partition hash=" << key.partitionHash());
			        auto& part = _pgetter.getPartitionForKey(key);
			        dto::K23SIWriteRequest<DataType> request;
			        request.pvid = part.partition->pvid;
			        request.collectionName = cname;
			        request.mtr = mtr;
			        request.trh = trh;
			        request.isDelete = isDelete;
			        request.designateTRH = isTRH;
			        request.key = key;
			        request.value.val = data;
			        return RPC().callRPC<dto::K23SIWriteRequest<DataType>, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, request, *part.preferredEndpoint, 100ms);
				} else {
					K2INFO("doWrite::checkCollection failed. collection \"" << cname << "\" is not exist. doWrite exit(1) with error CODE: " << retCode);
					Status s{.code = retCode, .message=""};
					dto::K23SIWriteResponse r;
					return RPCResponse(std::move(s), std::move(r));
				}
			});	
	}


public:		// test	scenario

// Any request (READ, WRITE, PUSH, END, FINALIZE, HEARTBEAT) should observe a timeout(404_not_found)
// example of command: CPO_COLLECTION_GET & K23SI_WRITE
seastar::future<> testScenario00() {
	K2INFO("+++++++ TestScenario 00: unassigned nodes +++++++");
	K2INFO("--->Test SETUP: start a cluster but don't create a collection. Example of command: CPO_COLLECTION_GET & K23SI_WRITE");
	
	auto request = dto::CollectionGetRequest{.name=collname};
	return checkCollection(collname)	// command: CPO_COLLECTION_GET 
		// response: CPO_COLLECTION_GET 
		.then([](auto&& retCode) {
			K2EXPECT(retCode, Statuses::S404_Not_Found.code);
			K2INFO("retCode:" << retCode);
		})
		.then([this] {
			// command: K23SI_WRITE
			K2INFO("command: K23SI_WRITE");
			return getTimeNow()
				.then([&](dto::Timestamp&& ts) {
					return seastar::do_with(
						dto::K23SI_MTR{
							.txnid = txnids++,
							.timestamp = std::move(ts),
							.priority = dto::TxnPriority::Medium
						},
						dto::Key{.partitionKey = "pKey1", .rangeKey = "rKey1"},
						dto::Key{.partitionKey = "pKey1", .rangeKey = "rKey1"},
						DataRec{.f1="field1", .f2="field2"},
						[this](dto::K23SI_MTR& mtr, dto::Key& key, dto::Key& trh, DataRec& rec){
							return doWrite<DataRec>(key, rec, mtr, trh, collname, false, true);
						}
					);
				});
			
		})
		.then([this](auto&& response) {
			// response: K23SI_WRITE
			auto& [status, resp] = response;
			K2INFO("response: K23SI_WRITE. " << "status: " << status.code);
		});
}


// create a collection and then get the collection
/*seastar::future<> testScenario01() {
	K2INFO(">>> TestScenario 00: assigned node with no data");
	K2INFO("--->Test setup: start a cluster and assign collection. Do not write any data.");

	
}



seastar::future<> testScenario02() {
	
}

seastar::future<> testScenario03() {
	
}

seastar::future<> testScenario04() {

}

seastar::future<> testScenario05() {

}
*/


};	// class k23si_testing
}	// ns k2

int main(int argc, char** argv){
	k2::App app("txn_testing");
	app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
	app.addApplet<k2::txn_testing>();
	return app.start(argc, argv);
}

