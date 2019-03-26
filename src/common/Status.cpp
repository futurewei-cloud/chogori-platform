#include "Status.h"

namespace k2
{

#define K2_STATUS_TEXT_APPLY(StatusName, StatusString) #StatusString,

const char *const statusText[(int)Status::StatusCount]{
    K2_STATUS_DEFINITION(K2_STATUS_TEXT_APPLY)};

} //  namespace k2