#pragma once

#include "Payload.h"

// General purpose macro for creating serializable structures of any field types.
// You have to pass your fields here in order for them to be (de)serialized. This macro works for any
// field types (both primitive/simple as well as nested/complex) but it does the (de)serialization
// on a field-by-field basis so it may be less efficient than the one-shot macro below
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


// This is a macro which can be put on structures which are directly copyable
// i.e. structures which can be copied by just casting the struct instance to a void* and
// copying some bytes:
// (void*)&structInstance, sizeof(structInstance)
// This is not going to work for structs which have complex nested fields (e.g. strings) as the content
// of the such nested field is in a different memory location. Use the general purpose macro above for
// these cases.
// In general, the simple type structs which can use K2_PAYLOAD_COPYABLE would be faster to serialize/deserialize
// as the entire struct is loaded/serialized in one single memory copy.
#define K2_PAYLOAD_COPYABLE struct __K2PayloadCopyableTraitTag__ {};
