#pragma once

// std
#include <vector>

namespace k2
{
namespace config
{

class PartitionManagerConfig
{
friend class ConfigParserLegacy;
protected:
    std::vector<std::string> _endpoints;

public:
   const std::vector<std::string>& getEndpoints()
   {
       return _endpoints;
   }

}; // class PartitionManagerConfig

}; // namespace config
}; // namespace k2
