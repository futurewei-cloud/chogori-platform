#include "Node.h"
namespace k2 {

void Node::setLocationInfo(String name, std::vector<String> endpoints_, int coreId)
{
    endPoints = std::move(endpoints_);
    String endPointsText;
    for(const auto& endpoint : endPoints)
        endPointsText += endpoint + ";";
    K2INFO("Initialized node " << name << ". Core:" << coreId << ". Endpoints:" << endPointsText);
}

Node::Node(INodePool& pool, NodeEndpointConfig nodeConfig) :
    assignmentManager(pool), nodeConfig(std::move(nodeConfig)) { }

const NodeEndpointConfig& Node::getEndpoint() const { return nodeConfig; }

const std::vector<String>& Node::getEndpoints() const { return endPoints; }

const String& Node::getName() const { return name; }

bool Node::processTasks() //  Return true if at least on task was processed
{
    processedRounds++;
    return assignmentManager.processTasks();
}

//
//  Pool monitor periodically checks whether Nodes are progress in their work:
//  with each check it looks whether variable is more than 0 (meaning we ran some rounds) and then set it to zero.
//  No atomic usage here, since we don't care about immediate consistency
//
uint32_t Node::getProcessedRoundsSinceLastCheck() const { return processedRounds; }

uint32_t Node::resetProcessedRoundsSinceLastCheck()
{
    auto result = processedRounds;
    processedRounds = 0;
    return result;
}

}   //  namespace k2
