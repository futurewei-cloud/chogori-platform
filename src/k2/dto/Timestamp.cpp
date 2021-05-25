/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include "Timestamp.h"

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
    uint64_t cnt = nsec(d).count();
    if (_tEndTSECount <= cnt + _tStartDelta) {
        // our clock hasn't started that far back. Return minimum value
        return Timestamp(_tStartDelta, _tsoId, _tStartDelta);
    }
    return Timestamp(_tEndTSECount - cnt, _tsoId, _tStartDelta);
}

Timestamp Timestamp::operator+(const Duration d) const {
    return Timestamp(_tEndTSECount + nsec(d).count(), _tsoId, _tStartDelta);
}

size_t Timestamp::hash() const {
    return hash_combine(_tEndTSECount, _tStartDelta, _tsoId);
}

bool Timestamp::operator==(const Timestamp& other) const noexcept {
    // certain and uncertain have the same behavior when the timestamps are equal
    return compareUncertain(other) == Timestamp::EQ;
}

bool Timestamp::operator!=(const Timestamp& other) const noexcept {
    return !operator==(other);
}

} // ns dto
} // ns k2
