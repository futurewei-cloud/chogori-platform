//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->
#pragma once

#include <unordered_map>
#include <vector>
#include <tuple>

#include <seastar/core/future.hh>  // for future stuff

#include <k2/common/Chrono.h>
#include <k2/dto/Collection.h>
#include <k2/transport/RPCTypes.h>
#include <k2/transport/Status.h>
#include <k2/transport/TXEndpoint.h>

namespace k2 {

class CPOClient {
public:
    CPOClient(String cpo_url);

    // Creates a collection and waits for it to be assigned. If the collection already exisits,
    // the future is still completed successfully
    seastar::future<Status> CreateAndWaitForCollection(Deadline<> deadline, dto::CollectionMetadata&& metadata, std::vector<String>&& clusterEndpoints);

    seastar::future<Status> GetAssignedPartitionWithRetry(Deadline<> deadline, const String& name, const dto::Key& key, uint8_t retries=1);

    // Gets the partition endpoint for request's key, executes the request, and refreshes the 
    // partition map if necessary
    template<class RequestT, typename ResponseT, Verb verb>
    seastar::future<std::tuple<Status, ResponseT>> PartitionRequest(Deadline<> deadline, RequestT& request, uint8_t tries_left=2);

    std::unique_ptr<TXEndpoint> cpo;
    std::unordered_map<String, dto::PartitionGetter> collections;

private:
    void FulfillWaiters(const String& name, const Status& status);
    std::unordered_map<String, std::vector<seastar::promise<Status>>> requestWaiters;
};

} // ns k2
