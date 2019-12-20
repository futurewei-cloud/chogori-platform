#pragma once

#include "PartitionMeta.h"

namespace k2 {

struct PartitionMapApp {
    seastar::distributed<PartitionMapApp> Dist_t;
    seastar::distributed<PartitionMap> pmaps;
    seastar::future<> start(){
        // TODO
        // Here, we should start p
    }
    seastar::future<> stop(){
        // TODO
    }
    PartitionMap& getPMap(){return pmaps.local();}
};

} // namespace k2
