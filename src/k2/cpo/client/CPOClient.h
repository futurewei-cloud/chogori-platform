//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->
#pragma once

#include <unordered_map>
#include <vector>
#include <tuple>

#include <seastar/core/future.hh>  // for future stuff
#include <seastar/core/sleep.hh>

#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/Collection.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RPCTypes.h>
#include <k2/transport/Status.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>

namespace k2 {

class CPOClient {
public:
    CPOClient(String cpo_url);
    CPOClient() = default;

    // Creates a collection and waits for it to be assigned. If the collection already exisits,
    // the future is still completed successfully
    template<typename ClockT=Clock>
    seastar::future<Status> CreateAndWaitForCollection(Deadline<ClockT> deadline, dto::CollectionMetadata&& metadata, std::vector<String>&& clusterEndpoints) {
        dto::CollectionCreateRequest request{.metadata = std::move(metadata),
                                             .clusterEndpoints = std::move(clusterEndpoints)};

        Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
        K2DEBUG("making call to CPO with timeout " << timeout);
        return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, request, *cpo, timeout).then([this, name = request.metadata.name, deadline](auto&& response) {
            auto& [status, k2response] = response;

            if (status == Status::S403_Forbidden() || status.is2xxOK()) {
                Duration s = std::min(deadline.getRemaining(), cpo_request_backoff());
                return seastar::sleep(s).then([this, name, deadline]() -> seastar::future<Status> {
                    return GetAssignedPartitionWithRetry(deadline, name, dto::Key{.partitionKey = "", .rangeKey = ""});
                });
            }

            return seastar::make_ready_future<Status>(std::move(status));
        });
    }

    // Get collection info from CPO, and retry if the partition for the given key
    // is not assigned or if there was a retryable error. It allows only one outstanding
    // request for a given collection.
    template <typename ClockT=Clock>
    seastar::future<Status> GetAssignedPartitionWithRetry(Deadline<ClockT> deadline, const String& name, const dto::Key& key, uint8_t retries = 1) {
        // Check if request is already issued, if so add to waiters and return
        K2DEBUG("time remaining=" << deadline.getRemaining() << ", for coll=" << name);
        auto it = requestWaiters.find(name);
        if (it != requestWaiters.end()) {
            K2DEBUG("found existing waiter");
            it->second.emplace_back(seastar::promise<Status>());
            return it->second.back().get_future().then([this, deadline, name, key, retries](Status&& status) {
                K2DEBUG("waiter finished with status: " << status);
                if (status.is2xxOK()) {
                    dto::Partition* partition = collections[name].getPartitionForKey(key).partition;
                    if (partition && partition->astate == dto::AssignmentState::Assigned) {
                        return seastar::make_ready_future<Status>(std::move(status));
                    }
                }

                if (!retries) {
                    status = Status::S408_Request_Timeout("Retries exceeded");
                    return seastar::make_ready_future<Status>(std::move(status));
                }

                return GetAssignedPartitionWithRetry(deadline, std::move(name), std::move(key), retries - 1);
            });
        }
        K2DEBUG("no existing waiter for name=" << name << ". Creating new one");

        // Register the ongoing request
        requestWaiters[name] = std::vector<seastar::promise<Status>>();

        Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
        dto::CollectionGetRequest request{.name = name};

        return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *cpo, timeout).then([this, name = request.name, key, deadline, retries](auto&& response) {
            auto& [status, coll_response] = response;
            bool retry = false;
            K2DEBUG("collection get response received with status: " << status);
            if (status.is2xxOK()) {
                collections[name] = dto::PartitionGetter(std::move(coll_response.collection));
                dto::Partition* partition = collections[name].getPartitionForKey(key).partition;
                FulfillWaiters(name, status);
                if (!partition || partition->astate != dto::AssignmentState::Assigned) {
                    K2DEBUG("No partition or not assigned: " << partition);
                    retry = true;
                }
            } else if (status.is5xxRetryable()) {
                retry = true;
            } else {
                FulfillWaiters(name, status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (!retry) {
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (status.is2xxOK() && retry && !retries) {
                status = Status::S503_Service_Unavailable("Not all partitions assigned");
                FulfillWaiters(name, status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (deadline.isOver()) {
                status = Status::S408_Request_Timeout("Deadline exceeded");
                FulfillWaiters(name, status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (!retries) {
                FulfillWaiters(name, status);
                status = Status::S408_Request_Timeout("Retries exceeded");
                return seastar::make_ready_future<Status>(std::move(status));
            }

            Duration s = std::min(deadline.getRemaining(), cpo_request_backoff());
            return seastar::sleep(s).then([this, name, key, deadline, retries]() -> seastar::future<Status> {
                return GetAssignedPartitionWithRetry(deadline, std::move(name), std::move(key), retries - 1);
            });
        });
    }

    // Gets the partition endpoint for request's key, executes the request, and refreshes the
    // partition map and retries if necessary. The caller must keep the request alive for the
    // duration of the future.
    // RequestT must have a pvid field and a collectionName field
    template<class RequestT, typename ResponseT, Verb verb, typename ClockT=Clock>
    seastar::future<std::tuple<Status, ResponseT>> PartitionRequest(Deadline<ClockT> deadline, RequestT& request, uint8_t retries=1) {
        K2DEBUG("making partition request with deadline=" << deadline.getRemaining());
        // If collection is not in cache or partition is not assigned, get collection first
        seastar::future<Status> f = seastar::make_ready_future<Status>(Status::S200_OK());
        auto it = collections.find(request.collectionName);
        if (it == collections.end()) {
            K2DEBUG("Collection not found");
            f = GetAssignedPartitionWithRetry(deadline, request.collectionName, request.key);
        } else {
            K2DEBUG("Collection found");
            dto::Partition* partition = collections[request.collectionName].getPartitionForKey(request.key).partition;
            if (!partition || partition->astate != dto::AssignmentState::Assigned) {
                K2DEBUG("Collection found but is in bad state");
                f = GetAssignedPartitionWithRetry(deadline, request.collectionName, request.key);
            }
        }

        return f.then([this, deadline, &request, retries](Status&& status) {
            K2DEBUG("Collection get completed with status: " << status);
            auto it = collections.find(request.collectionName);

            if (it == collections.end()) {
                // Failed to get collection, returning status from GetAssignedPartitionWithRetry
                K2DEBUG("Failed to get collection: " << status);
                return RPCResponse(std::move(status), ResponseT());
            }

            // Try to get partition info
            auto& partition = collections[request.collectionName].getPartitionForKey(request.key);
            if (!partition.partition || partition.partition->astate != dto::AssignmentState::Assigned) {
                // Partition is still not assigned after refresh attempts
                K2DEBUG("Failed to get assigned partition");
                return RPCResponse(Status::S503_Service_Unavailable("Partition not assigned"), ResponseT());
            }

            Duration timeout = std::min(deadline.getRemaining(), partition_request_timeout());
            request.pvid = partition.partition->pvid;
            K2DEBUG("making partition call to " << partition.preferredEndpoint->getURL() << ", with timeout=" << timeout);

            // Attempt the request RPC
            return RPC().callRPC<RequestT, ResponseT>(verb, request, *partition.preferredEndpoint, timeout).
            then([this, &request, deadline, retries] (auto&& result) {
                auto& [status, k2response] = result;
                K2DEBUG("partition call completed with status " << status);

                // Success or unrecoverable error
                if (status != Status::S410_Gone() && !status.is5xxRetryable()) {
                    return RPCResponse(std::move(status), std::move(k2response));
                }

                if (deadline.isOver()) {
                    K2DEBUG("Deadline exceeded");
                    status = Status::S408_Request_Timeout("Deadline exceeded");
                    return RPCResponse(std::move(status), ResponseT());
                }

                if (retries == 0) {
                    K2DEBUG("Retries exceeded, status: " << status);
                    return RPCResponse(Status::S408_Request_Timeout("Retries exceeded"), ResponseT());
                }

                // S410_Gone (refresh partition map) or retryable error
                return GetAssignedPartitionWithRetry(deadline, request.collectionName, request.key, 1)
                .then([this, &request, deadline, retries] (Status&& status) {
                    K2DEBUG("retrying partition call after status " << status);
                    (void) status;
                    return PartitionRequest<RequestT, ResponseT, verb>(deadline, request, retries-1);
                });
            });
        });
    }

    std::unique_ptr<TXEndpoint> cpo;
    std::unordered_map<String, dto::PartitionGetter> collections;

    ConfigDuration partition_request_timeout{"partition_request_timeout", 100ms};
    ConfigDuration cpo_request_timeout{"cpo_request_timeout", 100ms};
    ConfigDuration cpo_request_backoff{"cpo_request_backoff", 500ms};
private:
    void FulfillWaiters(const String& name, const Status& status);
    std::unordered_map<String, std::vector<seastar::promise<Status>>> requestWaiters;
};

} // ns k2
