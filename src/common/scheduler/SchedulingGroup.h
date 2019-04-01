#pragma once

// std
#include <iostream>
#include <string>
// seastar
#include <seastar/core/reactor.hh>

namespace k2
{

//
// Wrapper class for a seastar scheduling-group.
//
class SchedulingGroup {    
friend class RootScheduler;

private:
    std::string _name;
    seastar::scheduling_group _schedulingGroup;

public:
    SchedulingGroup(std::string name) {
        _name = std::move(name);
    }

    seastar::scheduling_group& getSchedulingGroup() {
        return _schedulingGroup;
    }

}; // class SchedulingGroup

} // namespace k2