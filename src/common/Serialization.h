#pragma once

#include "Payload.h"

#define K2_PAYLOAD_FIELDS(...)                        \
        struct __K2PayloadSerializableTraitTag__ {};  \
        bool writeFields(PayloadWriter& writer) const \
        {                                             \
            return writer.writeMany(__VA_ARGS__);     \
        }                                             \
        bool readFields(PayloadReader& reader)        \
        {                                             \
            return reader.readMany(__VA_ARGS__);      \
        }                                             \


#define K2_PAYLOAD_COPYABLE struct __K2PayloadCopyableTraitTag__ {};


namespace k2
{
template<typename MessageType, typename RequestType>
class MessageWithType
{
public:
    MessageType type;
    RequestType& request;

    MessageWithType(RequestType& request) : type(RequestType::getMessageType()), request(request) {}

    K2_PAYLOAD_FIELDS(type, request);
};

template<typename RequestType>
static auto makeMessageWithType(RequestType& request) { return MessageWithType<decltype(RequestType::getMessageType()), RequestType>(request); }
}

