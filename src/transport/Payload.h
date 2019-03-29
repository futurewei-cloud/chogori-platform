//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <vector>

#include "Fragment.h"

namespace k2tx {
class Payload {
public:
    Payload() = default;
    const std::vector<Fragment>& Fragments();
private:
    std::vector<Fragment> _fragments;
}; // class Payload
} // namespace k2tx
