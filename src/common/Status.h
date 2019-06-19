#pragma once

#include <iostream>
#include <signal.h>

namespace k2
{

#define CORE_DUMP(text)     {std::cerr << text << std::endl << std::flush; std::abort(); }

//  Asserts below should never be disabled and work on production also
#define ASSERT(condition)   {   if(!(condition)) CORE_DUMP(#condition);    }
#define ASSERT_TRUE(condition)   ASSERT(condition)

#define K2_STATUS_DEFINITION(STATUS)                                                                                    \
    STATUS(Ok, "Ok")                                                                                                    \
    STATUS(UnknownError, "Unknown error")                                                                                 \
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
    STATUS(AssignOperationIsBrokenByOffload, "Offload command received during assign process")                                      \
    STATUS(ModuleWithSuchIdAlreadyRegistered, "Attempt to register the module with duplicated id")                                      \
    STATUS(NoPartitionManagerSetup, "No partition manager settings")                                                  \
    STATUS(PartitionManagerSerializationError, "Partition manager serialization")                                      \
    STATUS(NodePoolHasNotYetBeenInitialized, "Node pool has not been yet initialized")                                      \
    STATUS(FailedToConnectToPartitionManager, "Connection to partition manager failed")                                      \

#define K2_STATUS_ENUM_APPLY(StatusName, StatusString)  StatusName,

enum class Status : uint32_t
{
    K2_STATUS_DEFINITION(K2_STATUS_ENUM_APPLY)
    StatusCount //  Count of statuses
};

extern const char* const statusText[(int)Status::StatusCount];

inline const char* getStatusText(Status status)
{
    ASSERT(status < Status::StatusCount);
    return statusText[(int)status];
}

inline Status logError(Status status, const char* fileName, int line, const char* function)
{
    std::cerr << "Error at " << fileName << ":" << line << "(" << function << ") " << getStatusText(status) << std::endl << std::flush;
    return status;
}

#define LOG_ERROR(status) k2::logError((status), __FILE__, __LINE__, __FUNCTION__)
#define RIF(status) { k2::Status ____status____ = (status); if(____status____ != k2::Status::Ok) return ____status____; }
#define RET(status) { k2::Status ____status____ = (status); return (____status____ != k2::Status::Ok) ? LOG_ERROR(____status____) : k2::Status::Ok; }
#define TIF(status) { k2::Status ____status____ = (status); if(____status____ != k2::Status::Ok) { LOG_ERROR(____status____); throw ____status____; } }

}  //  namespace k2


#define K2_STATUS_TEXT_APPLY(StatusName, StatusString)  #StatusString,
#define K2_DEFINE_STATUS_TEXT() namespace k2 { const char* const statusText[(int)Status::StatusCount] { K2_STATUS_DEFINITION(K2_STATUS_TEXT_APPLY) }; }

