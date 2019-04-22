#pragma once
#include <string>
#include <common/Message.h>

namespace k2
{
std::unique_ptr<ResponseMessage> sendMessage(const char* ip, uint16_t port, const Payload& message);
}