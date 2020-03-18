#pragma once

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>  // for future stuff

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/AssignmentManager.h>
#include <k2/transport/Status.h>

namespace k2 {

class CPOService {
private:
    typedef std::function<seastar::distributed<CPOService>&()> DistGetter;
    DistGetter _dist;
    ConfigVar<String> _dataDir{"data_dir"};
    String _getCollectionPath(String name);
    void _assignCollection(dto::Collection& collection);
    ConfigDuration _assignTimeout{"assignment_timeout", 10ms};
    ConfigDuration _collectionHeartbeatDeadline{"heartbeat_deadline", 100ms};
    std::unordered_map<String, seastar::future<>> _assignments;
    std::tuple<Status, dto::Collection> _getCollection(String name);
    Status _saveCollection(dto::Collection& collection);
    void _handleCompletedAssignment(const String& cname, dto::AssignmentCreateResponse&& request);

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
};  // class CPOService

} // namespace k2
