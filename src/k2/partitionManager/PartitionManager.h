#pragma once

// third-party
#include <k2/common/Common.h>
#include <k2/dto/Collection.h>
#include <k2/module/k23si/module.h>
#include <seastar/core/distributed.hh>  // for dist stuff
#include <seastar/core/future.hh>       // for future stuff

namespace k2 {

class PartitionManager {
public: // application lifespan
    PartitionManager();
    ~PartitionManager();
    seastar::future<dto::Partition> assignPartition(const String& cname, dto::Partition partition);

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();

private:
    std::unique_ptr<K23SIPartitionModule> _pmodule;
}; // class PartitionManager

// per-thread/reactor instance of the partition manager
extern __thread PartitionManager * __local_pmanager;
inline PartitionManager& PManager() { return *__local_pmanager; }
} // namespace k2
