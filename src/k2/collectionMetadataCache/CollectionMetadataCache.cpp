#include "CollectionMetadataCache.h"
#include <k2/common/Log.h>

namespace k2 {

CollectionMetadataCache::CollectionMetadataCache() {
    K2INFO("ctor");
}

CollectionMetadataCache::~CollectionMetadataCache() {
    K2INFO("dtor");
}

seastar::future<> CollectionMetadataCache::stop() {
    K2INFO("stop");
    return seastar::make_ready_future<>();
}

seastar::future<> CollectionMetadataCache::start() {
    return seastar::make_ready_future<>();
}

}  // namespace k2
