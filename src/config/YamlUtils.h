#pragma once

// yaml
#include <yaml-cpp/yaml.h>
// k2
#include <common/Log.h>

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

    static YAML::Node emptyMapIfNull(const YAML::Node& node)
    {
        return (node) ? node : YAML::Node(YAML::NodeType::Map);
    }

    static YAML::Node mergeAnchors(const YAML::Node& source)
    {
        if(!source) {
            return source;
        }

        auto node = source["<<"];
        if(!node || !source.IsMap() || !node.IsMap()) {

            return source;
        }

        if(node["<<"]) {
            node = mergeAnchors(node);
        }

        auto merged = YAML::Node(YAML::NodeType::Map);
        for(auto n: source) {
            if(n.first.as<std::string>() == "<<") {
                continue;
            }
            merged[n.first] = n.second;
        }

        for(auto n: node) {
            if(!merged[n.first.as<std::string>()]) {
                merged[n.first.as<std::string>()] = n.second;
            }
        }

        return std::move(merged);
    }

}; // struct YamlUtils

}; // namespace config
}; // namespace k2
