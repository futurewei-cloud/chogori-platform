#pragma once

// std
#include <map>
// k2:config
#include "NodePoolConfig.h"

namespace k2
{

class Config
{
friend class ConfigLoader;

protected:
    std::unordered_map<std::string, k2_shared_ptr<NodePoolConfig>> _nodepools;

public:
    Config()
    {
        // empty
    }

    std::vector<k2_shared_ptr<NodePoolConfig>> getNodePools()
    {
        std::vector<k2_shared_ptr<NodePoolConfig>> vector;

        for(auto entry : _nodepools) {
            vector.push_back(entry.second);
        }

        return std::move(vector);
    }

    void addNodePool(k2_shared_ptr<NodePoolConfig> pPoolConfig)
    {
        std::string id = (pPoolConfig->id).empty() ? std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) : pPoolConfig->id;
        auto pair = std::make_pair<std::string, k2_shared_ptr<NodePoolConfig>>(std::move(id), std::move(pPoolConfig));
        ASSERT(_nodepools.insert(std::move(pair)).second);
    }

}; //class Config

}; // namespace k2
