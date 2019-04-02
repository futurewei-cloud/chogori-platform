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