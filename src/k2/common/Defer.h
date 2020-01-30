#pragma once
#include "Log.h"

namespace k2 {

// Simple utility class used to defer execution of a function until the end of a scope
// the function is executed when the Defer instance is destroyed.

template <typename Func>
class Defer {
public:
    Defer(Func&& func) : _func(std::forward<Func>(func)) {}
    ~Defer() {
        try {
            (void) _func();
        }
        catch(...) {
            K2ERROR("deferred func threw exception");
        }
    }
private:
    Func _func;
};

} // ns k2
