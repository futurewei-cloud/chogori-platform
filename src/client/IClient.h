#pragma once

#include <cstdint>

namespace k2
{
namespace client
{
class IClient;

//
//  Configuration of K2 client
//
//  K2 client needs a thread pool to execute, so configuration need information on how many threads to use
//
class ClientSettings
{
public:
    std::string networkProtocol;
    std::string k2ClusterEndpoint;  //  URL of the cluster Partition Manager
    uint8_t networkThreadCount = 1; //  How many threads to use for network processing
    bool userInitThread = false;    //  If set to true, thread that is calling client init will be used as one of network threads

    //  Function will be called from client network thread. Returns number of microseconds after which it can be scheduled again.
    std::function<uint64_t(IClient&)> runInLoop;
};

//
//  Range of primary keys to which apply operation
//
class Range
{
    std::string lowKey;
    std::string highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
public:
    const std::string& getLowKey() const { return lowKey; }

    const std::string& getHighKey() const { return highKey; }

    bool isSingleKey() const { return highKey.empty() && lowKeyInclusive && highKeyInclusive; }

    bool isHighKeyInclusive() const { return highKeyInclusive; }

    bool isLowKeyInclusive() const { return lowKeyInclusive; }

    Range(std::string lowKey, bool lowKeyInclusive, std::string highKey, bool highKeyInclusive) :
        lowKey(lowKey), highKey(highKey), lowKeyInclusive(lowKeyInclusive), highKeyInclusive(highKeyInclusive) {}

    static Range singleKey(std::string key)
    {
        return Range(std::move(key), true, std::string(), true);
    }

    static Range less(std::string key)
    {
        return Range(std::string(), false, std::move(key), false);
    }

    static Range lessOrEqual(std::string key)
    {
        return Range(std::string(), false, std::move(key), true);
    }

    static Range greater(std::string key)
    {
        return Range(std::move(key), false, std::string(), false);
    }

    static Range greaterOrEqual(std::string key)
    {
        return Range(std::move(key), true, std::string(), false);
    }

    static Range close(std::string lowKey, std::string highKey)
    {
        return Range(std::move(lowKey), true, std::move(highKey), true);
    }

    static Range open(std::string lowKey, std::string highKey)
    {
        return Range(std::move(lowKey), false, std::move(highKey), false);
    }
};

//
//  Define the message that needs to be sent to set of ranges.
//
class Message
{
public:
    CollectionId collectionId;  //  Collection to which message is sent
    std::vector<Range> ranges;  //  Set of ranges within the collection
    Payload content;            //  Message content. Need to be allocated through client.createPayload()
};

//
//  Operation for K2 to execute. Operation consists of set of the messages that are sent to different key ranges of
//  different collections.
//
class Operation
{
public:
    std::vector<Message> messages;

    //  Number of variables used for sharing state between partitions in distributed transaction
    uint16_t shareStateVariableCount = 0;

    //  Whether to execute it as a transaction or as just batch of independent messages, which succedd when
    //  all of the messages succeed
    bool transactional = true;
};

//
//  Result of the operation
//
class OperationResponse
{
public:
    Status status;
    uint32_t moduleCode = 0;
    Payload payload;
};

class OperationResult
{
public:
    std::vector<OperationResponse> _responses;
};

//
//  Interaface for the client
//
class IClient
{
public:
    //
    //  Initialize the client
    //
    virtual void init(const ClientSettings& settings) = 0;

    //
    //  Execute user's operation
    //
    virtual void execute(Operation&& settings, std::function<void(IClient&, OperationResult&&)> onCompleted) = 0;

    //
    //  Execute user's operation for a singe partition.
    //
    virtual void execute(PartitionDescription& partition, std::function<void(Payload&)> onPayload, std::function<void(IClient&, OperationResult&&)> onCompleted) = 0;

    //
    //  Execute task within the client network thread pool. This can make task execution more efficient.
    //
    virtual void runInThreadPool(std::function<void(IClient&)> routine) = 0;

    //
    //  Create payload for particular message
    //
    virtual Payload createPayload() = 0;

    //
    //  Create payload for particular message (async)
    //
    virtual void createPayload(std::function<void(IClient&, Payload&&)> onCompleted) = 0;

    //
    //  Destructor
    //
    virtual ~IClient() {}
};

}   //  namespace client
}   //  namespace k2
