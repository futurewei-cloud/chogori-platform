#pragma once

#include "AssignmentManager.h"
#include "NodePool.h"
#include <memory>
#include <atomic>

namespace k2
{
//
//  Address configuration for endpoint
//
class Node
{
    DISABLE_COPY_MOVE(Node)
public:
    AssignmentManager assignmentManager;
protected:
    NodeEndpointConfig nodeConfig;
    uint32_t processedRounds = 0;
public:
    Node(INodePool& pool, NodeEndpointConfig nodeConfig) :
        assignmentManager(pool), nodeConfig(std::move(nodeConfig)) { }

    const NodeEndpointConfig& getEndpoint() const { return nodeConfig; }

    void processTasks()
    {
        processedRounds++;
        assignmentManager.processTasks();
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
