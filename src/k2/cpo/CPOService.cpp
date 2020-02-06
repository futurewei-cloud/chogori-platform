#include "CPOService.h"
#include <k2/transport/Payload.h>  // for payload construction
#include <k2/transport/Status.h>  // for RPC
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/dto/ControlPlaneOracle.h> // our DTO
#include <k2/dto/AssignmentManager.h> // our DTO
#include <k2/dto/MessageVerbs.h> // our DTO
#include <k2/transport/PayloadFileUtil.h>

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

namespace k2 {

CPOService::CPOService(DistGetter distGetter) : _dist(distGetter) {
    K2INFO("ctor");
}

CPOService::~CPOService() {
    K2INFO("dtor");
}

seastar::future<> CPOService::stop() {
    K2INFO("stop");
    std::vector<seastar::future<>> futs;
    for (auto& [k,v]: _assignments) {
        futs.push_back(std::move(v));
    }
    _assignments.clear();
    return seastar::when_all(futs.begin(), futs.end()).discard_result();
}

seastar::future<> CPOService::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, [this](dto::CollectionCreateRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleCreate, std::move(request));
    });

    RPC().registerRPCObserver<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, [this](dto::CollectionGetRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleGet, std::move(request));
    });

    RPC().registerRPCObserver<dto::AssignmentReportRequest, dto::AssignmentReportResponse>(dto::Verbs::CPO_REPORT_PARTITION_ASSIGNMENT, [this](dto::AssignmentReportRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleReportAssignment, std::move(request));
    });

    _dataDir = Config()["data_dir"].as<std::string>();
    if (seastar::engine().cpu_id() == 0) {
        // only core 0 handles CPO business
        if (!fileutil::makeDir(_dataDir)) {
            throw std::runtime_error("unable to create data directory");
        }
    }
    return seastar::make_ready_future<>();
}

seastar::future<std::tuple<Status, dto::CollectionCreateResponse>>
CPOService::handleCreate(dto::CollectionCreateRequest&& request) {
    K2INFO("Received collection create request for " << request.metadata.name);
    auto cpath = _getCollectionPath(request.metadata.name);
    if (fileutil::fileExists(cpath)) {
        return RPCResponse(Status::S403_Forbidden("Collection already exists"), dto::CollectionCreateResponse());
    }
    // create a collection from the incoming request
    dto::Collection collection;
    collection.metadata = request.metadata;
    auto& eps = request.clusterEndpoints;
    const uint64_t max = std::numeric_limits<uint64_t>::max();

    uint64_t partSize = (eps.size() > 0) ? (max / eps.size()) : (max);
    for (uint64_t i =0; i < eps.size(); ++i) {
        uint64_t start = i*partSize;
        uint64_t end = (i == eps.size() -1) ? (max) : ((i+1)*partSize - 1);

        dto::Partition part{
            .rangeVersion=1,
            .assignmentVersion=1,
            .startKey=std::to_string(start),
            .endKey=std::to_string(end),
            .endpoint=eps[i],
            .astate=dto::AssignmentState::PendingAssignment
        };
        collection.partitionMap.partitions.push_back(std::move(part));
        collection.partitionMap.version++;
    }
    auto status = _saveCollection(collection);
    if (!status.is2xxOK()) {
        return RPCResponse(std::move(status), dto::CollectionCreateResponse());
    }

    K2INFO("Created collection: " << cpath);
    _assignCollection(collection);
    return RPCResponse(std::move(status), dto::CollectionCreateResponse());
}

seastar::future<std::tuple<Status, dto::CollectionGetResponse>>
CPOService::handleGet(dto::CollectionGetRequest&& request) {
    K2INFO("Received collection get request for " << request.name);
    auto [status, collection] = _getCollection(request.name);

    dto::CollectionGetResponse response;
    if (status.is2xxOK()) {
        response.collection = std::move(collection);
    }
    return RPCResponse(std::move(status), std::move(response));
}

String CPOService::_getCollectionPath(String name) {
    return _dataDir + "/" + name + ".collection";
}

void CPOService::_assignCollection(dto::Collection& collection) {
    auto &name = collection.metadata.name;
    K2INFO("Assigning collection " << name << ", to " << collection.partitionMap.partitions.size() << " nodes");
    std::vector<seastar::future<>> futs;
    for (auto& part : collection.partitionMap.partitions) {
        K2INFO("Assigning collection " << name << ", to " << part.endpoint);
        auto txep = RPC().getTXEndpoint(part.endpoint);
        if (!txep) {
            K2WARN("unable to obtain endpoint for " << part.endpoint);
            continue;
        }
        dto::AssignmentCreateRequest request;
        request.collectionName = name;
        request.partition = part;
        futs.push_back(
        RPC().callRPC<dto::AssignmentCreateRequest, dto::AssignmentCreateResponse>
                (dto::K2_ASSIGNMENT_CREATE, std::move(request), *txep, _assignTimeout())
        .then([name, ep=part.endpoint ](auto result) {
            auto& [status, resp] = result;
            (void) resp;
            // The node refused to accept the assignment. For now, just ignore this
            K2WARN("assignment for collection " << name << " was refused by " << ep << ", due to: " << status);
            return seastar::make_ready_future();
        })
        );
    }
    _assignments.emplace(name, seastar::when_all(futs.begin(), futs.end()).discard_result()
        .then([this, name] {
            _assignments.erase(name);
            return seastar::make_ready_future();
        }));
}

seastar::future<std::tuple<Status, dto::AssignmentReportResponse>>
CPOService::handleReportAssignment(dto::AssignmentReportRequest&& request) {
    auto [status, haveCollection] = _getCollection(request.collectionName);
    if (!status.is2xxOK()) {
        return RPCResponse(Status::S404_Not_Found("assignment completion for non-existent collection"), dto::AssignmentReportResponse());
    }
    for (auto& part: haveCollection.partitionMap.partitions) {
        if (part.startKey == request.assignedPartition.startKey &&
            part.endKey == request.assignedPartition.endKey &&
            part.assignmentVersion == request.assignedPartition.assignmentVersion &&
            part.rangeVersion == request.assignedPartition.rangeVersion) {
                K2INFO("Assignment received for active partition " << request.assignedPartition);
                part.astate = request.assignedPartition.astate;
                return RPCResponse(_saveCollection(haveCollection), dto::AssignmentReportResponse());
        }
    }
    return RPCResponse(Status::S404_Not_Found("assignment completion does not match stored partition"), dto::AssignmentReportResponse());
}

std::tuple<Status, dto::Collection> CPOService::_getCollection(String name) {
    auto cpath = _getCollectionPath(name);
    std::tuple<Status, dto::Collection> result;
    Payload p;
    if (!fileutil::readFile(p, cpath)) {
        std::get<0>(result) = Status::S404_Not_Found("Collection not found");
        return result;
    }
    if (!p.read(std::get<1>(result))) {
        std::get<0>(result) = Status::S500_Internal_Server_Error("Unable to read collection data");
        return result;
    };
    K2INFO("Found collection in: " << cpath);
    std::get<0>(result) = Status::S200_OK();
    return result;
}

Status CPOService::_saveCollection(dto::Collection& collection) {
    auto cpath = _getCollectionPath(collection.metadata.name);
    Payload p([] { return Binary(4096); });
    p.write(collection);
    if (!fileutil::writeFile(std::move(p), cpath)) {
        return Status::S500_Internal_Server_Error("Unable to write collection data");
    }

    K2DEBUG("saved collection: " << cpath);
    return Status::S201_Created();
}

} // namespace k2
