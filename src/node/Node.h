#pragma once

#include "AssignmentManager.h"

namespace k2
{

//
//  Static configuration of the node. TODO: loading, etc.
//
class Node
{
public:
    AssignmentManager assignmentManager;


    static Node* getInstance() { return nullptr; }
};

}   //  namespace k2
