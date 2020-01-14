#pragma once
#include <k2/transport/RPCTypes.h>

namespace k2 {
namespace dto {
// These are the knowns verbs in K2. Since verbs are just ints for performance reasons, to make sure we do not
// have a mismatch between different builds, we keep all the verbs in one place
enum Verbs : k2::Verb {
    // ControlPlaneOracle: asked to create a collection
    CPO_COLLECTION_CREATE = 100,
    // ControlPlaneOracle: asked to return an existing collection
    CPO_COLLECTION_GET,
    // ControlPlaneOracle: a K2 node reports the results of partition assignment
    CPO_REPORT_PARTITION_ASSIGNMENT,
    // K2Assignment: CPO asks K2 to assign a partition
    K2_ASSIGNMENT_CREATE,
    // K2Assignment: CPO asks K2 to offload a partition
    K2_ASSIGNMENT_OFFLOAD,
    // K2Node: receives a message destined for a partition
    K2_PARTITION_MESSAGE,
};

} // namespace dto
} // namespace k2
