#include "Persistence.h"

namespace k2 {

Persistence::Persistence() {
    //TODO discover RDMA endpoint
    _remoteEndpoint = RPC().getTXEndpoint(_config.persistenceEndpoint());
    K2INFO("ctor with endpoint: " << _remoteEndpoint->getURL());
}

}
