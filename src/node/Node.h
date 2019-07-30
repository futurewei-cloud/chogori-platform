#pragma once

#include "AssignmentManager.h"
#include "NodePool.h"
#include <memory>
#include <atomic>
#include "common/Log.h"

namespace k2
{
//
//  Address configuration for endpoint
//
class Node
{
    DISABLE_COPY_MOVE(Node)
    friend class NodePoolImpl;
public:
    AssignmentManager assignmentManager;
protected:
    k2_shared_ptr<NodeEndpointConfig> pNodeConfig;
    uint32_t processedRounds = 0;

    std::vector<String> endPoints;
    String name;

    friend class NodePoolImpl;
    void setLocationInfo(String name, std::vector<String> endpoints_, int coreId)
    {
        endPoints = std::move(endpoints_);
        String endPointsText;
        for(const auto& endpoint : endPoints)
            endPointsText += endpoint + ";";
        K2INFO("Initialized node " << name << ". Core:" << coreId << ". Endpoints:" << endPointsText);
    }

public:
    Node(INodePool& pool, k2_shared_ptr<NodeEndpointConfig> nodeConfig) :
        assignmentManager(pool), pNodeConfig(nodeConfig) { /* empty */ }

    const NodeEndpointConfig& getEndpoint() const { return *pNodeConfig; }

    const std::vector<String>& getEndpoints() const { return endPoints; }

    const String& getName() const { return name; }

    bool processTasks() //  Return true if at least on task was processed
    {
        processedRounds++;
        return assignmentManager.processTasks();
    }

    //
    //  Pool monitor periodically checks whether Nodes are progress in their work:
    //  with each check it looks whether variable is more than 0 (meaning we ran some rounds) and then set it to zero.
    //  No atomic usage here, since we don't care about immediate consistency
    //
    uint32_t getProcessedRoundsSinceLastCheck() const { return processedRounds; }

    uint32_t resetProcessedRoundsSinceLastCheck()
    {
        auto result = processedRounds;
        processedRounds = 0;
        return result;
    }
};

//
//  Return Node that active within current thread context. Implementation is platform specific
//
Node& getActiveNode();

}   //  namespace k2
