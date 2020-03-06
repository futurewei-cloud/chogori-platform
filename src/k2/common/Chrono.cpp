#include "Chrono.h"

namespace k2 {

thread_local TimePoint CachedSteadyClock::_now = Clock::now();

CachedSteadyClock::time_point CachedSteadyClock::now(bool refresh) noexcept {
    if (refresh) {
        auto now = Clock::now();
        if (now > _now) {
            // make sure we're steady - only update value if we haven't gone past the real now()
            _now = now;
        }
    } else {
        _now += Duration(1ns);
    }
    return _now;
}
} // ns k2
