#pragma once

#include "TaskRequest.h"

namespace k2
{
//
//  Node partition manager (single per Node)
//
class AssignmentManager
{
protected:
    std::array<std::pair<PartitionId, std::unique_ptr<Partition>>, Constants::MaxCountOfPartitionsPerNode> partitions;  //  List of currently assigned partitions
    int partitionCount = 0; //  Count of currently assigned partitions

public:
    void process(std::unique_ptr<Message> message) {}   //  TODO: implement logic based on message
};

}   //  namespace k2
