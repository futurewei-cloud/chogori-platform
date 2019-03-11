#pragma once

#include <iostream>

namespace k2
{

#define TERMINATE(text)     { std::cerr << text << std::endl << std::flush; std::abort(); }
#define ASSERT(condition)   {   if(!(condition)) TERMINATE(#condition);    }

#define K2_STATUS_DEFINITION(STATUS)                                                                                    \
    STATUS(Ok, "Ok")                                                                                                    \
    STATUS(UnkownError, "Unkown error")                                                                                 \
    STATUS(MessageParsingError, "Failed to parse a message")                                                            \
    STATUS(PartitionAlreadyAssigned, "Attempt to assign partition which is already assigned")                           \
    STATUS(TooManyPartitionsAlready, "Partition cannot be assigned to the shard due exceeding the limit of partitions") \

#define K2_STATUS_ENUM_APPLY(StatusName, StatusString)  StatusName,

enum class Status : uint32_t
{
    K2_STATUS_DEFINITION(K2_STATUS_ENUM_APPLY)
    StatusCount //  Count of statuses
};

extern const char* const statusText[(int)Status::StatusCount];

const char* getStatusText(Status status)
{
    ASSERT(status < Status::StatusCount);
    return statusText[(int)status];
}

};  //  namespace k2
