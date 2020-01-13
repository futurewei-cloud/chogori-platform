#pragma once
#include <k2/transport/RPCTypes.h>

namespace k2 {
namespace dto {
// These are the knowns verbs in K2. Since verbs are just ints for performance reasons, to make sure we do not
// have a mismatch between different builds, we keep all the verbs in one place
enum Verbs : k2::Verb {
    CPO_COLLECTION_CREATE=100,
    CPO_COLLECTION_GET,
};

} // namespace dto
} // namespace k2
