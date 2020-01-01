#pragma once

#include "Payload.h"

#define K2_PAYLOAD_FIELDS(...)                        \
        struct __K2PayloadSerializableTraitTag__ {};  \
        void writeFields(Payload& payload) const \
        {                                             \
            payload.writeMany(__VA_ARGS__);     \
        }                                             \
        bool readFields(Payload& payload)        \
        {                                             \
            return payload.readMany(__VA_ARGS__);      \
        }                                             \


#define K2_PAYLOAD_COPYABLE struct __K2PayloadCopyableTraitTag__ {};
