#pragma once

#include <k2/dto/K23SI.h>
#include <k2/dto/TSO.h>
#include <k2/transport/PayloadSerialization.h>

namespace k2 {

// Used to store records in memory
struct DataRecord {
    dto::Key key;
    SerializeAsPayload<Payload> value;
    bool isTombstone = false;
    dto::Timestamp timestamp;
    dto::K23SI_MTR mtr;
    dto::Key trh;
    enum Status {
        WriteIntent, // the record hasn't been committed/aborted yet
        Committed // the record has been committed and we should use the key/value
        // aborted WIs don't need state - as soon as we learn that a WI has been aborted, we remove it
    } status;
};

} // ns k2
