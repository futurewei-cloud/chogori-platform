#pragma once

#include "Module.h"

namespace k2
{

//
//  Shared across all pools collection metadata
//
class CollectionManager
{
protected:
public:
    CollectionPtr getCollection(CollectionId collectionId) { return CollectionPtr(); }
};

//
//  
//
class NodePool
{
public:
    CollectionManager collectionManager;

    static NodePool* getInstance();
};

}   //  namespace k2
