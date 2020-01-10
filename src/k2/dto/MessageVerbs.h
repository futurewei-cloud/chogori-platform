#pragma once
#include <k2/transport/RPCTypes.h>

namespace k2 {
namespace dto {
// These are the knowns verbs in K2
enum Verbs : k2::Verb {
    CPO_COLLECTION_CREATE=100,
    CPO_COLLECTION_GET,
};

} // namespace dto
} // namespace k2
