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

#pragma once

#include <unordered_map>
#include <vector>
#include <tuple>

#include <seastar/core/future.hh>  // for future stuff
#include <seastar/core/sleep.hh>

#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/Collection.h>
#include <k2/dto/PersistenceCluster.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RPCTypes.h>
#include <k2/transport/Status.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/LogStream.h>

namespace k2::cpo {
namespace log {
inline thread_local logging::Logger cpoclient("k2::cpo_client");
}

class CPOClient {
public:
    CPOClient();
    ~CPOClient();

    void init(String cpoURL);

    // Creates a collection and waits for it to be assigned. If the collection already exisits,
    // the future is still completed successfully
    template<typename ClockT=Clock>
    seastar::future<Status> createAndWaitForCollection(Deadline<ClockT> deadline, dto::CollectionMetadata&& metadata, std::vector<String>&& rangeEnds) {
        dto::CollectionCreateRequest request{.metadata = std::move(metadata),
                                             .rangeEnds = std::move(rangeEnds)};

        Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
        K2LOG_D(log::cpoclient, "making call to CPO with timeout {}", timeout);
        return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, request, *cpo, timeout).then([this, name = request.metadata.name, deadline](auto&& response) {
            auto& [status, k2response] = response;

            if (status == Statuses::S403_Forbidden || status.is2xxOK()) {
                Duration s = std::min(deadline.getRemaining(), cpo_request_backoff());
                return seastar::sleep(s).then([this, name, deadline]() -> seastar::future<Status> {
                    return _getAssignedPartitionWithRetry(deadline, name, dto::Key{});
                });
            }

            return seastar::make_ready_future<Status>(std::move(status));
        });
    }

    // Gets the partition endpoint for request's key, executes the request, and refreshes the
    // partition map and retries if necessary. The caller must keep the request alive for the
    // duration of the future.
    // RequestT must have a pvid field and a collectionName field
    template<class RequestT, typename ResponseT, Verb verb, typename ClockT=Clock>
    seastar::future<std::tuple<Status, ResponseT>>
    partitionRequest(Deadline<ClockT> deadline, RequestT& request,
                     bool reverse=false, bool exclusiveKey=false, uint8_t retries=1) {
        K2LOG_D(log::cpoclient, "making partition request with deadline={}", deadline.getRemaining());

        return _getPartitionGetterForCollection(deadline, request.collectionName, request.key, reverse, exclusiveKey)
        .then([this, deadline, &request, reverse, exclusiveKey, retries](auto&& result) {
            auto& [status, pgetter] = result;
            K2LOG_D(log::cpoclient, "Collection get completed with status={}, request={} ", status, request);
            if (!status.is2xxOK()) {
                // Failed to get collection, returning status from GetAssignedPartitionWithRetry
                K2LOG_D(log::cpoclient, "Failed to get collection with status={}", status);
                return RPCResponse(std::move(status), ResponseT{});
            }
            // Try to get partition info
            auto& partition = pgetter->getPartitionForKey(request.key, reverse, exclusiveKey);
            return _partitionRequestByPartition<RequestT, ResponseT, verb, ClockT>(partition, deadline, request)
            .then([this, deadline, &request, reverse, exclusiveKey, retries] (auto&& result) {
                auto& [status, k2resp] = result;
                if (status.is5xxRetryable()) {
                    K2LOG_D(log::cpoclient, "call failed with retryable status: {}", status);
                    if (retries == 0) {
                        K2LOG_W(log::cpoclient, "Retries exceeded, status={}", status);
                        return RPCResponse(Statuses::S408_Request_Timeout("partition retries exceeded"), ResponseT{});
                    }
                    K2LOG_D(log::cpoclient, "refreshing collection from CPO after status={}", status);
                    return _getAssignedPartitionWithRetry(deadline, request.collectionName, request.key, reverse, exclusiveKey)
                    .then([this, deadline, &request, reverse, exclusiveKey, retries] (auto&& status) {
                        // just retry here regardless of the result.
                        K2LOG_D(log::cpoclient, "retrying partition call after status={}", status);
                        return partitionRequest<RequestT, ResponseT, verb, ClockT>(deadline, request, reverse, exclusiveKey, retries - 1);
                    });
                }
                K2LOG_D(log::cpoclient, "call completed with status: {}", status);

                return RPCResponse(std::move(status), std::move(k2resp));
            });
        });
    }

    // Similar to partitionRequest, but it routes the request by the partition with the given pvid.
    // Unlike partitionRequest(), this method does not retry to send the request if there is a
    // partition remapping - the caller is responsible of figuring out the correct pvid to use in cases of remap.
    template<class RequestT, typename ResponseT, Verb verb, typename ClockT=Clock>
    seastar::future<std::tuple<Status, ResponseT>>
    partitionRequestByPVID(Deadline<ClockT> deadline, RequestT& request, uint8_t retries=1) {
        return _getPartitionGetterForCollection(deadline, request.collectionName)
        .then([this, deadline, &request, retries] (auto&& result) {
            auto& [status, pgetter] = result;
            K2LOG_D(log::cpoclient, "Collection get completed with status={}, request={} ", status, request);
            if (!status.is2xxOK()) {
                // Failed to get collection, returning status from GetAssignedPartitionWithRetry
                K2LOG_D(log::cpoclient, "Failed to get collection with status={}", status);
                return RPCResponse(std::move(status), ResponseT{});
            }
            auto* partition = pgetter->getPartitionForPVID(request.pvid);
            if (!partition) {
                return RPCResponse(Statuses::S410_Gone(fmt::format("no partition for pvid {}", request.pvid)), ResponseT{});
            }
            return _partitionRequestByPartition<RequestT, ResponseT, verb, ClockT>(*partition, deadline, request)
            .then([this, &request, deadline, retries] (auto&& result) {
                auto& [status, k2resp] = result;
                if (status.is5xxRetryable()) {
                    K2LOG_D(log::cpoclient, "call failed with retryable status: {}", status);
                    if (retries == 0) {
                        K2LOG_W(log::cpoclient, "Retries exceeded, status={}", status);
                        return RPCResponse(Statuses::S408_Request_Timeout("partition retries exceeded"), ResponseT{});
                    }
                    K2LOG_D(log::cpoclient, "retrying partition call after status={}", status);
                    return partitionRequestByPVID<RequestT, ResponseT, verb, ClockT>(deadline, request, retries-1);
                }
                K2LOG_D(log::cpoclient, "call completed with status: {}", status);

                return RPCResponse(std::move(status), std::move(k2resp));
            });
        });
    }

    template<typename ClockT=Clock>
    seastar::future<std::tuple<Status, dto::PersistenceClusterGetResponse>>
    getPersistenceCluster(Deadline<ClockT> deadline, String name) {
        dto::PersistenceClusterGetRequest request{.name = std::move(name)};

        Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
        return RPC().callRPC<dto::PersistenceClusterGetRequest, dto::PersistenceClusterGetResponse>(dto::Verbs::CPO_PERSISTENCE_CLUSTER_GET, request, *cpo, timeout)
        .then([this, &request, deadline] (auto&& result) {
            auto& [status, k2response] = result;

            if (deadline.isOver()) {
                K2LOG_D(log::cpoclient, "Deadline exceeded");
                status = Statuses::S408_Request_Timeout("persistence deadline exceeded");
                return RPCResponse(std::move(status), dto::PersistenceClusterGetResponse());
            }

            return RPCResponse(std::move(status), std::move(k2response));
        });
    }

    seastar::future<k2::Status> createSchema(const String& collectionName, k2::dto::Schema schema);
    seastar::future<std::tuple<k2::Status, std::vector<k2::dto::Schema>>> getSchemas(const String& collectionName);

    std::unique_ptr<TXEndpoint> cpo;
    std::unordered_map<String, seastar::lw_shared_ptr<dto::PartitionGetter>> collections;

    template<typename ClockT=Clock>
    seastar::future<std::tuple<Status, dto::MetadataPutResponse>> PutPartitionMetadata(Deadline<ClockT> deadline, String partitionName, uint32_t sealed_offset, String new_plogId) {
        dto::MetadataPutRequest request{.partitionName = std::move(partitionName), .sealed_offset=std::move(sealed_offset), .new_plogId=std::move(new_plogId)};

        Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
        return RPC().callRPC<dto::MetadataPutRequest, dto::MetadataPutResponse>(dto::Verbs::CPO_PARTITION_METADATA_PUT, request, *cpo, timeout);
    }

    template<typename ClockT=Clock>
    seastar::future<std::tuple<Status, dto::MetadataGetResponse>> GetPartitionMetadata(Deadline<ClockT> deadline, String partitionName) {
        dto::MetadataGetRequest request{.partitionName = std::move(partitionName)};

        Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
        return RPC().callRPC<dto::MetadataGetRequest, dto::MetadataGetResponse>(dto::Verbs::CPO_PARTITION_METADATA_GET, request, *cpo, timeout);
    }

    ConfigDuration partition_request_timeout{"partition_request_timeout", 100ms};
    ConfigDuration schema_request_timeout{"schema_request_timeout", 1s};
    ConfigDuration cpo_request_timeout{"cpo_request_timeout", 100ms};
    ConfigDuration cpo_request_backoff{"cpo_request_backoff", 500ms};
private:
    template <typename ClockT=Clock>
    seastar::future<std::tuple<Status, seastar::lw_shared_ptr<dto::PartitionGetter>>>
    _getPartitionGetterForCollection(Deadline<ClockT> deadline, const String& cname, const dto::Key& key=dto::Key{},
                                    bool reverse=false, bool exclusiveKey=false) {
        // If collection is not in cache or partition is not assigned, get collection first
        seastar::future<Status> f = seastar::make_ready_future<Status>(Statuses::S200_OK("default cached response"));
        auto it = collections.find(cname);
        if (it == collections.end()) {
            K2LOG_D(log::cpoclient, "Collection {} not found", cname);
            f = _getAssignedPartitionWithRetry(deadline, cname, key, reverse, exclusiveKey);
        } else {
            K2LOG_D(log::cpoclient, "Collection found");
            dto::Partition* partition = it->second->getPartitionForKey(key, reverse, exclusiveKey).partition;
            if (!partition || partition->astate != dto::AssignmentState::Assigned) {
                K2LOG_D(log::cpoclient, "Collection {} found but is in bad state", cname);
                f = _getAssignedPartitionWithRetry(deadline, cname, key, reverse, exclusiveKey);
            }
        }
        return f.then([this, &cname] (auto&& status) {
            auto it = collections.find(cname);
            if (it == collections.end()) {
                // Failed to get collection, returning status from GetAssignedPartitionWithRetry
                K2LOG_D(log::cpoclient, "Failed to get collection with status={}", status);
                return RPCResponse(std::move(status), seastar::lw_shared_ptr<dto::PartitionGetter>());
            }
            return RPCResponse(Statuses::S200_OK(""), seastar::lw_shared_ptr<dto::PartitionGetter>(it->second));
        });
    }

    // Get collection info from CPO, and retry if the partition for the given key
    // is not assigned or if there was a retryable error. It allows only one outstanding
    // request for a given collection.
    template <typename ClockT=Clock>
    seastar::future<Status>
    _getAssignedPartitionWithRetry(Deadline<ClockT> deadline, const String& name, const dto::Key& key,
                                    bool reverse=false, bool excludedKey=false, uint8_t retries=1) {
        // Check if request is already issued, if so add to waiters and return
        K2LOG_D(log::cpoclient, "time remaining={}, for coll={}, key={}, reverse={}, excludedKey={}, retries={}", deadline.getRemaining(), name, key, reverse, excludedKey, retries);
        auto it = requestWaiters.find(name);
        if (it != requestWaiters.end()) {
            K2LOG_D(log::cpoclient, "found existing waiter");
            it->second.emplace_back(seastar::promise<Status>());
            return it->second.back().get_future().then([this, deadline, name, key, reverse, excludedKey, retries](Status&& status) {
                K2LOG_D(log::cpoclient, "waiter finished with status={}", status);
                if (status.is2xxOK()) {
                    dto::Partition* partition = collections[name]->getPartitionForKey(key, reverse, excludedKey).partition;
                    if (partition && partition->astate == dto::AssignmentState::Assigned) {
                        return seastar::make_ready_future<Status>(std::move(status));
                    }
                    K2LOG_W(log::cpoclient, "Partition found but still not completed assignment");
                }
                else {
                    K2LOG_W(log::cpoclient, "Partition not found with status={}", status);
                }

                if (!retries) {
                    status = Statuses::S408_Request_Timeout("get assigned partition retries exceeded");
                    return seastar::make_ready_future<Status>(std::move(status));
                }

                return _getAssignedPartitionWithRetry(deadline, std::move(name), std::move(key), reverse, excludedKey, retries - 1);
            });
        }
        K2LOG_D(log::cpoclient, "no existing waiter for name={}. Creating new one", name);

        // Register the ongoing request
        requestWaiters[name] = std::vector<seastar::promise<Status>>();

        Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
        dto::CollectionGetRequest request{.name = name};

        return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *cpo, timeout).then([this, name = request.name, key, deadline, reverse, excludedKey, retries](auto&& response) {
            auto& [status, coll_response] = response;
            bool retry = false;
            K2LOG_D(log::cpoclient, "collection get response received with status={}, for name={}", status, name);
            if (status.is2xxOK()) {
                collections[name] = seastar::make_lw_shared<dto::PartitionGetter>(std::move(coll_response.collection));
                dto::Partition* partition = collections[name]->getPartitionForKey(key, reverse, excludedKey).partition;
                _fulfillWaiters(name, status);
                if (!partition || partition->astate != dto::AssignmentState::Assigned) {
                    K2LOG_D(log::cpoclient, "No partition or not assigned");
                    retry = true;
                }
            } else if (status.is5xxRetryable()) {
                retry = true;
            } else {
                K2LOG_D(log::cpoclient, "non-retryable error");
                _fulfillWaiters(name, status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (!retry) {
                K2LOG_D(log::cpoclient, "retry not needed");
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (status.is2xxOK() && retry && !retries) {
                K2LOG_D(log::cpoclient, "not all partitions have been assigned in cpo yet");
                status = Statuses::S503_Service_Unavailable("not all partitions assigned in cpo");
                _fulfillWaiters(name, status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (deadline.isOver()) {
                K2LOG_D(log::cpoclient, "deadline reached");
                status = Statuses::S408_Request_Timeout("cpo deadline exceeded");
                _fulfillWaiters(name, status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            if (!retries) {
                K2LOG_D(log::cpoclient, "all retries have been exhausted");
                _fulfillWaiters(name, status);
                status = Statuses::S408_Request_Timeout("cpo retries exceeded");
                return seastar::make_ready_future<Status>(std::move(status));
            }
            // refresh the cached clock so that we calculate deadlines better
            CachedSteadyClock::now(true);

            Duration s = std::min(deadline.getRemaining(), cpo_request_backoff());
            K2LOG_D(log::cpoclient, "will retry after {}", s);
            return seastar::sleep(s).then([this, name, key, deadline, reverse, excludedKey, retries]() -> seastar::future<Status> {
                // kill the waiters queue if it is empty so that the recursive call can be processed as the only
                // in-progress call
                if (auto it = requestWaiters.find(name); it != requestWaiters.end() && it->second.empty()) {
                    requestWaiters.erase(it);
                }
                return _getAssignedPartitionWithRetry(deadline, std::move(name), std::move(key), reverse, excludedKey, retries - 1);
            });
        });
    }

    template<class RequestT, typename ResponseT, Verb verb, typename ClockT=Clock>
    seastar::future<std::tuple<Status, ResponseT>>
    _partitionRequestByPartition(dto::PartitionGetter::PartitionWithEndpoint& partition, Deadline<ClockT> deadline, RequestT& request) {
        if (!partition.partition || partition.partition->astate != dto::AssignmentState::Assigned) {
            // Partition is still not assigned after refresh attempts
            K2LOG_D(log::cpoclient, "Failed to get assigned partition");
            return RPCResponse(Statuses::S503_Service_Unavailable("partition not assigned"), ResponseT());
        }

        Duration timeout = std::min(deadline.getRemaining(), partition_request_timeout());
        request.pvid = partition.partition->keyRangeV.pvid;
        K2LOG_D(log::cpoclient, "making partition call to url={}, with timeout={}", partition.preferredEndpoint->url, timeout);

        // Attempt the request RPC
        return RPC()
        .callRPC<RequestT, ResponseT>(verb, request, *partition.preferredEndpoint, timeout)
        .then([this, &request, deadline] (auto&& result) {
            auto& [status, k2response] = result;
            K2LOG_D(log::cpoclient, "partition call completed with status={}", status);

            // Success or unrecoverable error
            if (status != Statuses::S410_Gone && !status.is5xxRetryable()) {
                return RPCResponse(std::move(status), std::move(k2response));
            }

            if (deadline.isOver()) {
                K2LOG_D(log::cpoclient, "Deadline exceeded");
                return RPCResponse(Statuses::S408_Request_Timeout("partition deadline exceeded"), ResponseT{});
            }

            return RPCResponse(Statuses::S503_Service_Unavailable("unable to make call at this time"), ResponseT{});
        });
    }

    void _fulfillWaiters(const String& name, const Status& status);
    std::unordered_map<String, std::vector<seastar::promise<Status>>> requestWaiters;
};

} // ns k2
