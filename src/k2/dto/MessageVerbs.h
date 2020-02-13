#pragma once
#include <k2/transport/RPCTypes.h>

namespace k2 {
namespace dto {
// These are the knowns verbs in K2. Since verbs are just ints for performance reasons, to make sure we do not
// have a mismatch between different builds, we keep all the verbs in one place
enum Verbs : k2::Verb {
    // ControlPlaneOracle: asked to create a collection
    CPO_COLLECTION_CREATE = 10,
    // ControlPlaneOracle: asked to return an existing collection
    CPO_COLLECTION_GET,

    // K2Assignment: CPO asks K2 to assign a partition
    K2_ASSIGNMENT_CREATE = 20,
    // K2Assignment: CPO asks K2 to offload a partition
    K2_ASSIGNMENT_OFFLOAD,

    // K23SI reads
    K23SI_READ = 30,
    // K23SI writes
    K23SI_WRITE,
    // K23SI push operation
    K23SI_PUSH,
    // K23SI end transaction
    K23SI_END,
    // K23SI finalize transaction
    K23SI_TXN_FINALIZE
};

} // namespace dto
} // namespace k2
