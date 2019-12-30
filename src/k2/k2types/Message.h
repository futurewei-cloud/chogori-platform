#pragma once

#include <cassert>
#include "PartitionMetadata.h"
#include <k2/transport/Status.h>

namespace k2 {
//
//  Message traveled through the system
//
class Message {
   protected:
    Endpoint sender;

   public:
    Payload payload;

    Message() {}
    Message(Endpoint&& sender, Payload&& payload) : sender(std::move(sender)), payload(std::move(payload)) {}
};

//
//  Represent message sink to respond back to client
//
class IClientConnection {
   protected:
    Endpoint sender;

   public:
    //
    //  Send reponse to sender
    //
    virtual PayloadWriter getResponseWriter() = 0;

    //
    //  Send error to sender
    //
    virtual void sendResponse(Status status, uint32_t code = 0) = 0;

    //
    //  Return address of the sender
    //
    const Endpoint& getSender() { return sender; }

    //
    //  Destructor
    //
    virtual ~IClientConnection() {}
};

}  //  namespace k2
