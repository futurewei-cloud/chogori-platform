#pragma once

namespace k2
{

//
//  Since collection metadata is shared across all Nodes in a pool we need to make it shareble
//
enum class ModuleId : uint8_t
{
    None = 0,   //  Error
    Metadata,   //  Module handle metadata for collections, partitions, nodes, etc.
    Default,    //  Used for some default module
};

}   //  namespace k2
