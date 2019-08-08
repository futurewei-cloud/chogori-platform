#pragma once

// yaml
#include <yaml-cpp/yaml.h>

namespace k2
{
namespace config
{

class YamlUtils
{
public:
    template<class T>
    static T getOptionalValue(const YAML::Node& node, T defaultValue)
    {
        return node ? node.as<T>(defaultValue) : defaultValue;
    }

    template<class T>
    static T getRequiredValue(const YAML::Node& node, const std::string& name, T dataType)
    {
        YAML::Node valueNode = node[name];
        if(!valueNode) {
            K2ERROR("Config; missing config value:" << name);
            ASSERT(valueNode);
        }

        return getOptionalValue(valueNode, dataType);
    }
}; // struct YamlUtils

}; // namespace config
}; // namespace k2
