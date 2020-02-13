#pragma once
#include <k2/common/Common.h>

namespace k2
{
//
//  K2 internal MVCC representation
//
template<typename ValueType>
struct VersionedTreeNode {
    uint64_t version;
    ValueType value;
    std::unique_ptr<VersionedTreeNode<ValueType>> next;
};

}
