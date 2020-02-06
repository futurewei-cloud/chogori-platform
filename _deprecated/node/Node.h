#pragma once

#include <atomic>
#include <k2/common/Log.h>
#include <memory>
#include "AssignmentManager.h"
#include "NodePool.h"

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
    NodeEndpointConfig nodeConfig;
    uint32_t processedRounds = 0;

    std::vector<String> endPoints;
    String name;

    friend class NodePoolImpl;
    void setLocationInfo(String name, std::vector<String> endpoints_, int coreId);

public:
    Node(INodePool& pool, NodeEndpointConfig nodeConfig);

    const NodeEndpointConfig& getEndpoint() const;

    const std::vector<String>& getEndpoints() const;

    const String& getName() const;

    bool processTasks();

    //
    //  Pool monitor periodically checks whether Nodes are progress in their work:
    //  with each check it looks whether variable is more than 0 (meaning we ran some rounds) and then set it to zero.
    //  No atomic usage here, since we don't care about immediate consistency
    //
    uint32_t getProcessedRoundsSinceLastCheck() const;

    uint32_t resetProcessedRoundsSinceLastCheck();
};

}   //  namespace k2
