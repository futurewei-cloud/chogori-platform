#pragma once

#include "Chrono.h"
#include <k2/transport/Serialization.h>

namespace k2
{    
    enum TimeStampComparisonResult : int8_t {
        UN = -2,    // Unknown      - when compare TimeStamp from different TSO and uncertainty window overlaps
        LT = -1,    // Less than    - if two K2TimeStamp are of same TSOId,  this.Te < other.Te. If different TSOId, this.Te < other.Ts 
        EQ = 0,     // Equal        - if two K2TimeStamp are of same TSOId, this.Te == other.Te. This should be unusual.
        GT = 1,     // Greater than - if of same TSOId, this.Te > other.Te. If of different TSOId, this.Ts > other.Te.
    };

    // K2TimeStamp - a TrueTime uncertainty window and TSOId
    // intenally keep TEndTSECount and tStartDelta for efficient serialization and comparison
    class TimeStamp
    {
        public:
        // end time of uncertainty window;
        inline constexpr uint64_t TEndTSECount() { return _tEndTSECount; } ;
        inline constexpr SysTimePt TEnd()  { return SysTimePt_FromNanoTSE(_tEndTSECount); } ;     
        // start time of uncertainty window;
        inline constexpr uint64_t TStartTSECount()  {return _tEndTSECount - _tStartDelta; } ;
        inline constexpr SysTimePt TStart()  { return SysTimePt_FromNanoTSE(TStartTSECount()); };   
        // global unique Id of the TSO issuing this TS
        inline constexpr uint32_t TSOId() { return _tsoId; };                     

        constexpr TimeStampComparisonResult Compare(TimeStamp& other) 
        {
            if (TSOId() == other.TSOId())
            {
                return TEndTSECount() < other.TEndTSECount() ? LT :
                    (TEndTSECount() > other.TEndTSECount() ? GT : EQ);
            }
            else
            {
                return TEndTSECount() < other.TStartTSECount() ? LT :
                    (TStartTSECount() > other.TEndTSECount() ? GT : UN);
            }
        };

        TimeStamp(uint64_t tEndTSECount, uint32_t tsoId, uint16_t tStartDelta) :  // should verify tEndTSECount > tStartDelta 
            _tEndTSECount(tEndTSECount), _tsoId(tsoId), _tStartDelta(tStartDelta) {};

        private:
        
        uint64_t _tEndTSECount;         // nanosec count of tEnd's TSE
        uint32_t _tsoId;
        uint16_t _tStartDelta;          // TStart delta from TEnd in nanoseconds, 64K nanoseconds max

        K2_PAYLOAD_FIELDS(_tEndTSECount, _tsoId, _tStartDelta);
    };
}