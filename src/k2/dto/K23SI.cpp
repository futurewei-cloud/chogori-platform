#include "K23SI.h"
namespace k2 {
namespace dto {
const K23SI_MTR K23SI_MTR_ZERO;

bool K23SI_MTR::operator==(const K23SI_MTR& o) const {
    return txnid == o.txnid && timestamp.compareUncertain(o.timestamp) == Timestamp::EQ && priority == o.priority;
}
bool K23SI_MTR::operator!=(const K23SI_MTR& o) const {
    return !(operator==(o));
}

size_t K23SI_MTR::hash() const {
    return std::hash<decltype(txnid)>{}(txnid) + std::hash<decltype(priority)>{}(priority) + timestamp.hash();
}

} // ns dto
} // ns k2
