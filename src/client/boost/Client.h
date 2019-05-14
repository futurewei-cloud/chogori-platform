#pragma once
#include "common/Payload.h"
#include "common/Message.h"

namespace k2
{
std::unique_ptr<ResponseMessage> sendMessage(const char* ip, uint16_t port, Payload&& message);
}
