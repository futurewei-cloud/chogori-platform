#pragma once
#include <k2/appbase/AppEssentials.h>
#include "Config.h"

namespace k2 {
class Persistence {
public:
    Persistence();
    seastar::future<> makeCall(FastDeadline deadline);
private:
    std::unique_ptr<TXEndpoint> _remoteEndpoint;
    K23SIConfig _config;
};
}
