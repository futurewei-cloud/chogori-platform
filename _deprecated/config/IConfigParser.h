#pragma once

// k2
#include <k2/common/Common.h>
#include "Config.h"

namespace k2
{
namespace config
{

class IConfigParser
{
public:
    virtual std::shared_ptr<Config> parseConfig(const YAML::Node& node) = 0;

}; // class IConfigParser

}; // namespace config
}; // namespace k2
