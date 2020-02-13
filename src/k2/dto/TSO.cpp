#include "TSO.h"

namespace k2 {
namespace dto {

uint64_t Timestamp::TEndTSECount() const {
    return _tEndTSECount;
}

uint64_t Timestamp::TStartTSECount() const {
    return _tEndTSECount - _tStartDelta;
}

uint32_t Timestamp::TSOId() const {
    return _tsoId;
}

Timestamp::CompareResult Timestamp::Compare(const Timestamp& other) const {
    if (TSOId() == other.TSOId()) {
        return TEndTSECount() < other.TEndTSECount() ? LT :
        (TEndTSECount() > other.TEndTSECount() ? GT : EQ);
    }
    else {
        return TEndTSECount() < other.TStartTSECount() ? LT :
            (TStartTSECount() > other.TEndTSECount() ? GT : UN);
    }
}

} // ns dto
} // ns k2
