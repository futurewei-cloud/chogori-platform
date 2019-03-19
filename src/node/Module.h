#pragma once

#include "Partition.h"
#include <seastar/core/temporary_buffer.hh>

namespace k2
{

typedef seastar::temporary_buffer<uint8_t> ShareStateValue;

//
//  Represent transaction shared state calculate for particular partition
//
struct PartitionSharedState
{
    std::vector<ShareStateValue> state;
};


//
//  Represent shared state of the transaction
//
struct SharedState
{
    PartitionSharedState self;
    std::map<Endpoint, PartitionSharedState> remoteState;
};


//
//  IOOperation type
//
enum class IOOperationType
{
    Persist,
    Message
};


//
//  Describe request for IO operation
//
class IOOperation
{
protected:
    IOOperationType type;
    String address;
    Binary payload;
public:
    IOOperationType getType()
    {
        return type;
    }

    String getAddress()
    {
        return address;
    }

    const Binary& getPayload()
    {
        return payload;
    }
};


//
//  Class represent sink for IO operations
//
class IOOperations
{
public:
    virtual void registerIO(IOOperation&& operation) = 0;
};


//
//  Value returned from IModule API
//
struct ModuleRespone
{
    enum ResponseType : uint8_t
    {
        Ok = 0,     //  Can move to next state
        ReturnToClient, //  Return status to client immediately
        Error,      //  Some error occurred 
        Postpone,   //  Task need to continue later
        RescheduleAfterIOCompletion,     //  Continue when all IO operations are finished
    };

    ResponseType type;
    union
    {
        uint32_t resultCode;            //  If type == Error: contains Module specific result code
        uint32_t potponeDelayUs;        //  If type == Postpone: contains time in microseconds for which task needs to be delayed
    };
    
    ModuleRespone(ResponseType type, uint32_t value) : type(type), resultCode(value) { }
    ModuleRespone(ResponseType type) : type(type), resultCode(0) { }
    constexpr bool isOk() { return type != Error; }
};


//
//  Module is a core component of K2 Extensibility framework
//
class IModule
{
public:
    //
    //  Called when Node pool is loaded
    //
    virtual ModuleRespone OnInit() { return ModuleRespone::Ok; }

    //
    //  Called before Node pool get destroyed.
    //
    virtual ModuleRespone OnRelease() { return ModuleRespone::Ok; }

    //
    //  Called when Pool observe partition for collection it didn't see before.
    //  Can be called from any Node.
    //
    virtual ModuleRespone OnNewCollection(CollectionMetadata& collectionMetadata) { return ModuleRespone::Ok; }

    //
    //  Called when partition get assigned
    //
    virtual ModuleRespone OnAssign(AssignmentTask& assignment) { return ModuleRespone::Ok; }

    //
    //  Called when partition get offloaded
    //
    virtual ModuleRespone OnOffload(OffloadTask& offload) { return ModuleRespone::Ok; }

    //
    //  Called when client request is received. In this function Module can check whether operation can be completed,
    //  set shared state (if there are some) and log the transaction record through ioOperations.
    //  Module can respond with:
    //      Ok: To wait for IO finished and move to the next stage (either Coordinate or Apply)
    //      Error: To cancel the task with provided error code
    //      Postpone: To reschedule the task for some later time
    //      RescheduleAfterIOCompletion: To wait while all IOs are done and schedule task again
    //
    virtual ModuleRespone OnPrepare(ClientTask& task, IOOperations& ioOperations) = 0;

    //
    //  Called either for for distributed transactions after responses from all participants have been received. Module
    //  can aggregate shared state and make a decision on whether to proceed with transaction.
    //
    virtual ModuleRespone OnCoordinate(ClientTask& task, SharedState& remoteSharedState, IOOperations& ioOperations) = 0;

    //
    //  Called after OnPrepare stage is done (Module returned Ok and all IOs are finished). On this stage Module can
    //  apply it's transaction to update in memory representation or release locks.
    //
    virtual ModuleRespone OnApply(ClientTask& task) = 0;

    //
    //  Called when Module requests some maintainence jobs (e.g. snapshoting).
    //
    virtual ModuleRespone OnMaintainence(MaintainenceTask& task) { return ModuleRespone::Ok; }

    //
    //  Destructor. Called when Pool is terminated
    //
    virtual ~IModule() { }
};

}   //  namespace k2
