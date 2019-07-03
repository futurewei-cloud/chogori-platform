#include "PoolMonitor.h"
#include "Node.h"
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <transport/BoostTransport.h>


namespace k2
{
template<typename RequestT, typename ResponseT>
Status PoolMonitor::sendMessage(const RequestT& request, ResponseT& response)
{
    if(pool.getConfig().getPartitionManagerSet().empty())
        return LOG_ERROR(Status::NoPartitionManagerSetup);

    Status result;
    for(const auto& pm : pool.getConfig().getPartitionManagerSet())
    {
        try
        {
            //  TODO: Partition Manager can switch to different instance and let is know which is that. Also need to set time out
            BoostTransport::messageExchange(pm.c_str(), KnownVerbs::PartitionManager, makeMessageWithType(request), response);
            if(response.status != Status::Ok)
                continue;

            return Status::Ok;
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        catch(...)
        {
            std::cerr << "error\n";
        }

        result = Status::FailedToConnectToPartitionManager;
    }

    return result;
}


void PoolMonitor::run()
{
    try
    {
        registerNodePool();

        while(!pool.isTerminated())
        {
            std::this_thread::sleep_for(pool.getConfig().getMonitorSleepTime());

            //  Crash if any of the nodes didn't do any processing
            for(size_t i = 0; i < pool.getNodesCount(); i++)
                ASSERT(pool.getNode(i).resetProcessedRoundsSinceLastCheck());

            sendHeartbeat();
        }
    }
    catch(const std::exception& e)
    {
        state = State::failure;
        std::cerr << e.what() << '\n';
        ASSERT(false);
    }
    catch(...)
    {
        state = State::failure;
        ASSERT(false);
    }
}

const String tcpEndPointPrefix = "tcp+k2rpc://";

bool getTCPHostAndPort(const std::vector<String>& endpoints, String& tcpHostAndPort)
{
    for(const String& endpoint : endpoints)
    {
        if(
            endpoint.size() <= tcpEndPointPrefix.size() ||
            endpoint.substr(0, tcpEndPointPrefix.size()) != tcpEndPointPrefix)   //  TODO: in C++20 will be starts_with
            continue;

        tcpHostAndPort = endpoint.substr(endpoint.size());
        return true;
    }

    return false;
}

void PoolMonitor::registerNodePool()
{
    manager::NodePoolRegistrationMessage::Request registerRequest;
    manager::NodePoolRegistrationMessage::Response registerResponse;
    ASSERT(nodeTCPHostAndPorts.empty());

    registerRequest.poolId = pool.getName();
    for(size_t i = 0; i < pool.getNodesCount(); i++)
    {
        manager::NodeInfo node;
        node.endpoints = pool.getNode(i).getEndpoints();
        ASSERT(getTCPHostAndPort(node.endpoints, node.tcpHostAndPort));
        nodeTCPHostAndPorts.push_back(node.tcpHostAndPort);
        registerRequest.nodes.push_back(std::move(node));
    }

    TIF(sendMessage(registerRequest, registerResponse));

    sessionId = registerResponse.sessionId;
    nodeRegistrationIds = registerResponse.nodeIds;
    lastHeartbeat = TimePoint::now();
    state = State::active;
}

void PoolMonitor::sendHeartbeat()
{
    manager::HeartbeatMessage::Request heartbeatRequest;
    manager::HeartbeatMessage::Response heartbeatResponse;

    heartbeatRequest.poolId = pool.getName();
    for(size_t i = 0; i < pool.getNodesCount(); i++)
        heartbeatRequest.nodeNames.push_back(nodeTCPHostAndPorts[i]);
    heartbeatRequest.sessionId = sessionId;

    TIF(sendMessage(heartbeatRequest, heartbeatResponse));

    lastHeartbeat = TimePoint::now();
}

}   //  namespace k2
