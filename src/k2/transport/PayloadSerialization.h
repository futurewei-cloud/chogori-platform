/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include "Payload.h"

// General purpose macro for creating serializable structures of any field types.
// You have to pass your fields here in order for them to be (de)serialized. This macro works for any
// field types (both primitive/simple as well as nested/complex) but it does the (de)serialization
// on a field-by-field basis so it may be less efficient than the one-shot macro below
#define K2_PAYLOAD_FIELDS(...)                                             \
    struct __K2PayloadSerializableTraitTag__ {};                           \
    void __writeFields(k2::Payload& ___payload_local_macro_var___) const { \
        ___payload_local_macro_var___.writeMany(__VA_ARGS__);              \
    }                                                                      \
    bool __readFields(k2::Payload& ___payload_local_macro_var___) {        \
        return ___payload_local_macro_var___.readMany(__VA_ARGS__);        \
    }

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

// macro useful for serializing empty classes/structs. C++ standard demands size to be at least 1, and will inject
// a dummy char if class/struct is empty anyway.
// we do the same here, but initialize this empty char so that we can validate message content on both sides
#define K2_PAYLOAD_EMPTY                  \
    char ___empty_payload_char___ = '\0'; \
    struct __K2PayloadCopyableTraitTag__ {};
