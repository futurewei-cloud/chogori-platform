#include "NodePool.h"
#include "module/MemKVModule.h"
#include <node/seastar/SeastarPlatform.h>

using namespace k2;

int main(int argc, char** argv)
{
    (void)argc; // TODO use me
    (void)argv; // TODO use me
    try
    {
        k2::NodePool pool;  //  Configure pool based on argc and argv
        TIF(pool.registerModule(ModuleId::Default, std::make_unique<k2::MemKVModule<MapIndexer>>()));

        //  TODO: configure from files
        NodeEndpointConfig nodeConfig;
        nodeConfig.type = NodeEndpointConfig::IPv4;
        nodeConfig.ipv4.address = ntohl((uint32_t)inet_addr("127.0.0.1"));
        nodeConfig.ipv4.port = 11311;
        TIF(pool.registerNode(nodeConfig));

        SeastarPlatform platform;
        TIF(platform.run(pool));
    }
    catch(const Status& status)
    {
        LOG_ERROR(status);
        return (int)status;
    }
    catch(const std::runtime_error& re)
    {
        std::cerr << "Runtime error: " << re.what() << std::endl;
        return 1;
    }
    catch(const std::exception& ex)
    {
        std::cerr << "Error occurred: " << ex.what() << std::endl;
        return 1;
    }
    catch(...)
    {
        std::cerr << "Unknown exception is thrown." << std::endl;
        return 1;
    }

    return 0;
}
