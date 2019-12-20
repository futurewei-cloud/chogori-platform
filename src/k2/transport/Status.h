#pragma once

#include <signal.h>
#include <iostream>

#include <k2/common/Log.h>

namespace k2 {
#define K2_STATUS_DEFINITION(STATUS)                                                                                    \
    STATUS(Ok, "Ok")                                                                                                    \
    STATUS(UnknownError, "Unknown error")                                                                               \
    STATUS(TimedOut, "Timed out on the request")                                                                        \
    STATUS(SchedulerPlatformStartingFailure, "Some error during scheduler start")                                       \
    STATUS(MessageParsingError, "Failed to parse a message")                                                            \
    STATUS(PartitionAlreadyAssigned, "Attempt to assign partition which is already assigned")                           \
    STATUS(TooManyPartitionsAlready, "Partition cannot be assigned to the shard due exceeding the limit of partitions") \
    STATUS(IOOperationCanceled, "IO operation was canceled")                                                            \
    STATUS(IOOperationHasNotBeenFinished, "IO result is not available since it's still running")                        \
    STATUS(ModuleIsNotRegistered, "Request to assign partition to node which doesn't have requested module registered") \
    STATUS(ModuleRejectedCollection, "Module responded with failure on initializing collection")                        \
    STATUS(NodeNotServicePartition, "Request for partition that is not served by current Node")                         \
    STATUS(PartitionVersionMismatch, "Partition version in request doesn't match served partition version")             \
    STATUS(UnkownMessageType, "Unkown message type")                                                                    \
    STATUS(AssignOperationIsBrokenByOffload, "Offload command received during assign process")                          \
    STATUS(ModuleWithSuchIdAlreadyRegistered, "Attempt to register the module with duplicated id")                      \
    STATUS(NoPartitionManagerSetup, "No partition manager settings")                                                    \
    STATUS(PartitionManagerSerializationError, "Partition manager serialization")                                       \
    STATUS(NodePoolHasNotYetBeenInitialized, "Node pool has not been yet initialized")                                  \
    STATUS(FailedToConnectToPartitionManager, "Connection to partition manager failed")                                 \

#define K2_STATUS_ENUM_APPLY(StatusName, StatusString)  StatusName,

enum class Status : uint32_t {
    K2_STATUS_DEFINITION(K2_STATUS_ENUM_APPLY)
    StatusCount //  Count of statuses
};

extern const char* const statusText[(int)Status::StatusCount];

inline const char* getStatusText(Status status) {
    K2ASSERT(status < Status::StatusCount, "Invalid status: " << ((uint32_t)status));
    return statusText[(int)status];
}
inline std::ostream& operator<<(std::ostream& os, Status st) {
    os << getStatusText(st);
    return os;
}

#define RET_IF_BAD(status) { k2::Status ____status____ = (status); if(____status____ != k2::Status::Ok) return ____status____; }
#define THROW_IF_BAD(status) { k2::Status ____status____ = (status); if(____status____ != k2::Status::Ok) { throw ____status____; } }

}  //  namespace k2
