#include "K23SI.h"
namespace k2 {
namespace dto {
const K23SI_MTR K23SI_MTR_ZERO;

bool K23SI_MTR::operator==(const K23SI_MTR& o) const {
    return txnid == o.txnid && timestamp.compare(o.timestamp) == Timestamp::EQ && priority == o.priority;
}
bool K23SI_MTR::operator!=(const K23SI_MTR& o) const {
    return !(operator==(o));
}

} // ns dto
} // ns k2
