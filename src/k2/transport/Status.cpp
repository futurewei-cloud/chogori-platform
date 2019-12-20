#include "Status.h"

#define K2_STATUS_TEXT_APPLY(StatusName, StatusString) #StatusString,
#define K2_DEFINE_STATUS_TEXT()                                                                         \
    namespace k2 {                                                                                      \
    const char* const statusText[(int)Status::StatusCount]{K2_STATUS_DEFINITION(K2_STATUS_TEXT_APPLY)}; \
    }

K2_DEFINE_STATUS_TEXT();
