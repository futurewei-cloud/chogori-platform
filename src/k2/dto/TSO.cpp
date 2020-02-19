#include "TSO.h"

namespace k2 {
namespace dto {
Timestamp::Timestamp(uint64_t tEndTSECount, uint32_t tsoId, uint32_t tStartDelta):
    _tEndTSECount(tEndTSECount),
    _tsoId(tsoId),
    _tStartDelta(tStartDelta) {
}

uint64_t Timestamp::tEndTSECount() const {
    return _tEndTSECount;
}

uint64_t Timestamp::tStartTSECount() const {
    return _tEndTSECount - _tStartDelta;
}

uint32_t Timestamp::tsoId() const {
    return _tsoId;
}

Timestamp::CompareResult Timestamp::compare(const Timestamp& other) const {
    if (tsoId() == other.tsoId()) {
        return tEndTSECount() < other.tEndTSECount() ? LT :
        (tEndTSECount() > other.tEndTSECount() ? GT : EQ);
    }
    else {
        return tEndTSECount() < other.tStartTSECount() ? LT :
            (tStartTSECount() > other.tEndTSECount() ? GT : UN);
    }
}

Timestamp::CompareResult Timestamp::compareCertain(const Timestamp& other) const {
    auto result = compare(other);
    if (result == UN) {
        result = _tsoId < other._tsoId ? LT : GT;
    }
    return result;
}

} // ns dto
} // ns k2
