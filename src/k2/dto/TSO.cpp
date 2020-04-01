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

Timestamp::CompareResult Timestamp::compareUncertain(const Timestamp& other) const {
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
    if (tEndTSECount() != other.tEndTSECount()){
        return tEndTSECount() < other.tEndTSECount() ? LT : GT;
    }
    else {
        return tsoId() < other.tsoId() ? LT : 
            (tsoId() > other.tsoId() ? GT : EQ);
    }
}

Timestamp Timestamp::operator-(const Duration d) const {
    return Timestamp(_tEndTSECount - nsec(d).count(), _tsoId, _tStartDelta);
}

Timestamp Timestamp::operator+(const Duration d) const {
    return Timestamp(_tEndTSECount + nsec(d).count(), _tsoId, _tStartDelta);
}

} // ns dto
} // ns k2
