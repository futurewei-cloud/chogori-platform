#pragma once
#include <string>
#include <common/Message.h>

namespace k2
{
std::unique_ptr<ResponseMessage> sendMessage(const char* ipAndPort, const Payload& message);
}