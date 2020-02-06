#pragma once

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>  // for future stuff

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/transport/Status.h>

namespace k2 {

class CPOService {
private:
    typedef std::function<seastar::distributed<CPOService>&()> DistGetter;
    DistGetter _dist;
    String _dataDir;
    String _getCollectionPath(String name);
    void _assignCollection(dto::Collection& collection);
    ConfigDuration _assignTimeout{"assignment_timeout", 10ms};
    std::unordered_map<String, seastar::future<>> _assignments;
    std::tuple<Status, dto::Collection> _getCollection(String name);
    Status _saveCollection(dto::Collection& collection);

   public:  // application lifespan
    CPOService(DistGetter distGetter);
    ~CPOService();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();

    seastar::future<std::tuple<Status, dto::CollectionCreateResponse>>
    handleCreate(dto::CollectionCreateRequest&& request);

    seastar::future<std::tuple<Status, dto::CollectionGetResponse>>
    handleGet(dto::CollectionGetRequest&& request);

    seastar::future<std::tuple<Status, dto::AssignmentReportResponse>>
    handleReportAssignment(dto::AssignmentReportRequest&& request);
};  // class CPOService

} // namespace k2
