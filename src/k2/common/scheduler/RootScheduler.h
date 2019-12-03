#pragma once

// std
#include <iostream>
#include <map>
// seastar
#include <seastar/core/seastar.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/gate.hh>
// k2
#include "IScheduler.h"
#include "SchedulingGroup.h"

namespace k2
{

//
// The RootScheduler maintains a list of schedulers and gives equal turns to each of them. The RootScheduler is also responsible for managing the
// lifecycle of scheduling-groups.
//
class RootScheduler: public IScheduler {
private:
     std::unordered_map<std::string, seastar::shared_ptr<SchedulingGroup>> _schedulingGroups;
     std::vector<seastar::future<>> _futures;
     std::vector<seastar::shared_ptr<IScheduler>> _schedulers;
     seastar::shared_ptr<seastar::gate> _gatePtr;

public:
     RootScheduler() {
          _gatePtr = seastar::make_shared<seastar::gate>();
     };

     ~RootScheduler() {
          // left empty
     };

     //
     // Retrieves the Scheduling group with the specified name.
     //
     seastar::shared_ptr<SchedulingGroup> getSchedulingGroup(const std::string& name) {
          auto it = _schedulingGroups.find(name);
          if(_schedulingGroups.end()==it) {
               throw std::invalid_argument( "Scheduling group not found!" );
          }

          return it->second;
     }

     //
     // Registers the provided Scheduler.
     //
     void registerScheduler(seastar::shared_ptr<IScheduler> scheduler) {
          scheduler->_gatePtr = _gatePtr;
          _schedulers.push_back(scheduler);
     }

     //
     // Creates a scheduling group.
     //
     seastar::future<> createSchedulingGroup(const std::string& name, float shares) {
          auto it = _schedulingGroups.find(name);
          if(_schedulingGroups.end()!=it) {
               // scheduling group already exists
               return seastar::make_ready_future<>();
          }

          return seastar::create_scheduling_group(name, shares).then([this, name] (seastar::scheduling_group sg) {
               seastar::shared_ptr<SchedulingGroup> sgPtr = seastar::make_shared<SchedulingGroup>(name);
               sgPtr->_schedulingGroup = sg;
               _schedulingGroups.insert(std::pair(std::move(name), sgPtr));

               return seastar::make_ready_future<>();
          });
    }

     //
     // Starts all registered schedulers.
     //
     seastar::future<> start() {
          std::cout << "Starting root scheduler..." << std::endl;

          for(auto& scheduler : _schedulers) {
               _futures.push_back(scheduler->start());
          }

          // wait for all schedulers to terminate
          return seastar::when_all_succeed(_futures.begin(), _futures.end());
     }

     //
     // Stops all registered Schedulers.
     //
     void stop() {
          std::cout << "Stopping root scheduler..." << std::endl;

          for(auto& scheduler : _schedulers) {
               scheduler->stop();
          }

          _futures.clear();
     }

     //
     // Invoked when the service is terminated. Once closed, the RootScheduler cannot be restarted.
     //
     seastar::future<> close() {
          std::cout << "Closing root scheduler..." << std::endl;
          stop();

          return _gatePtr->close();
     }

}; // class RootScheduler

} // namespace k2
