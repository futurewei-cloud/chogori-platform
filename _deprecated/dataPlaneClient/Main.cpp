#include <iostream>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <k2/modules/memkv/server/MemKVModule.h>
#include <k2/k2types/PartitionMetadata.h>
#include <k2/common/TimeMeasure.h>

#include <k2/client/PartitionMessageTransport.h>
#include <boost/timer.hpp>
#include <time.h>

namespace k2
{

void moduleSet(k2::PartitionAssignmentId partitionId, const char* ip, uint16_t port, std::string&& key, std::string&& value)
{
    MemKVModule<>::SetRequest setRequest { std::move(key), std::move(value) };

    Stopwatch stopWatch;
    std::unique_ptr<ResponseMessage> response = sendPartitionMessage(ip, port,
        k2::MessageType::ClientRequest, partitionId, MemKVModule<>::RequestWithType(setRequest));

    K2INFO("moduleSet:" << stopWatch.elapsedUS()  << "us");

    assert(response);
    if(!response->getStatus().is2xxOK() || response->moduleCode != 0)
        K2ERROR("Set failed: " << response->getStatus());
}

void moduleGet(k2::PartitionAssignmentId partitionId, const char* ip, uint16_t port, std::string&& key)
{
    MemKVModule<>::GetRequest getRequest { std::move(key), std::numeric_limits<uint64_t>::max() };

    Stopwatch stopWatch;
    std::unique_ptr<ResponseMessage> response = sendPartitionMessage(ip, port,
        k2::MessageType::ClientRequest, partitionId, MemKVModule<>::RequestWithType(getRequest));

    K2INFO("moduleGet:" << stopWatch.elapsedUS()  << "us");

    assert(response);
    if(!response->getStatus().is2xxOK() || response->moduleCode != 0) {
        K2ERROR("Get failed: " << response->getStatus());
        return;
    }
    MemKVModule<>::GetResponse getResponse;
    if (!response->payload.read(getResponse)) {
        throw std::runtime_error("unable to read response in module");
    }
    K2INFO("Gotten: value: " << getResponse.value << " version: " << getResponse.version);
}

}

namespace bpo = boost::program_options;

void printHelp(bpo::options_description& desc, std::string& appName)
{
    K2INFO("Usage: " << appName << " (assign|offload|get|set) [options]");
    K2INFO(desc);
}

int main(int argc, char** argv)
{
    std::string partition, nodeIP, command, key, value;
    uint16_t nodePort;
    std::string appName = boost::filesystem::basename(argv[0]);

    bpo::options_description desc("Options");
    desc.add_options()
        ("help,h", "Print help messages")
        ("partition,p", bpo::value<std::string>(&partition)->required(), "Partition assignment id to send message to. Format PartitionId,RangeVersion,AssignmentVersion. E.g. '1.2.3'.")
        ("nodeIP,N", bpo::value<std::string>(&nodeIP)->default_value("127.0.0.1"), "IP address of Node to which partition belongs. 127.0.0.1 by default")
        ("nodePort,P", bpo::value<uint16_t>(&nodePort)->default_value(11311), "Port of Node to which partition belongs. 11311 by default")
        ("key,k", bpo::value<std::string>(&key), "Key for set and get commands")
        ("value,v", bpo::value<std::string>(&value), "Value for set command")
        ("command", bpo::value<std::string>(&command), "Command to execute: assign|offload|get|set");

    bpo::positional_options_description positionalOptions;
    positionalOptions.add("command", 1);
    bpo::variables_map arguments;

    k2::PartitionAssignmentId partitionId;

    try
    {
        bpo::store(bpo::command_line_parser(argc, argv).options(desc).positional(positionalOptions).run(), arguments);

        if(arguments.count("help"))
        {
            printHelp(desc, appName);
            return 0;
        }

        bpo::notify(arguments); // Throws exception if there are any problems

        K2INFO("Executing command " << command << " for Node:" << nodeIP << ":" << nodePort << " Partition:");

        if(!partitionId.parse(partition.c_str()))
        {
            K2ERROR("Cannot parse partition id");
            return 1;
        }
    }
    catch(std::exception& e)
    {
        K2ERROR("Unhandled exception while parsing arguments: " << e.what());
        printHelp(desc, appName);
        return 1;
    }

    try
    {
        if(command == "set")
            k2::moduleSet(partitionId, nodeIP.c_str(), nodePort, std::move(key), std::move(value));
        else if(command == "get")
            k2::moduleGet(partitionId, nodeIP.c_str(), nodePort, std::move(key));
        else
        {
            K2ERROR("Unknown command: " << command);
            printHelp(desc, appName);
            return 1;
        }

        K2INFO("Command \"" << command << "\" successfully executed.");
    }
    catch(std::exception& e)
    {
        K2ERROR("Unhandled exception while executing command: " << e.what());
        return 1;
    }
    catch(...)
    {
        K2ERROR("Unhandled exception while executing command");
        return 1;
    }

    return 0;
}