#include <iostream>

#include "boost/program_options.hpp"
#include "boost/filesystem.hpp"

#include <common/PartitionMetadata.h>

#include <node/module/MemKVModule.h>

#include <lib/Client.h>

namespace k2
{

constexpr CollectionId collectionId = 3;

void assignPartition(k2::PartitionAssignmentId partitionId, const char* ipAndPort)
{
    AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = CollectionMetadata(collectionId, ModuleId::Default, {});
    assignmentMessage.partitionMetadata = PartitionMetadata(partitionId.id, PartitionRange("A", "B"), collectionId);   //  TODO: change range
    assignmentMessage.partitionVersion = partitionId.version;

    std::unique_ptr<ResponseMessage> response = sendMessage(ipAndPort, PartitionMessage::serializeMessage(k2::MessageType::PartitionAssign, partitionId, assignmentMessage));
    if(response->getStatus() != Status::Ok)
        std::cerr << "Assignment failed: " << (int)response->getStatus() << std::endl << std::flush;
}

void offloadPartition(k2::PartitionAssignmentId partitionId, const char* ipAndPort)
{
    (void) partitionId; // TODO use me
    (void) ipAndPort; // TODO use me
}

void moduleSet(k2::PartitionAssignmentId partitionId, const char* ipAndPort, std::string&& key, std::string&& value)
{
    MemKVModule<>::SetRequest setRequest { std::move(key), std::move(value) };
    std::unique_ptr<ResponseMessage> response = sendMessage(ipAndPort,
        PartitionMessage::serializeMessage(k2::MessageType::ClientRequest, partitionId, MemKVModule<>::RequestWithType(setRequest)));
    if(response->getStatus() != Status::Ok || response->moduleCode != 0)
        std::cerr << "Set failed: " << (int)response->getStatus() << std::endl << std::flush;
}

void moduleGet(k2::PartitionAssignmentId partitionId, const char* ipAndPort, std::string&& key)
{
    MemKVModule<>::GetRequest getRequest { std::move(key), std::numeric_limits<uint64_t>::max() };
    std::unique_ptr<ResponseMessage> response = sendMessage(ipAndPort,
        PartitionMessage::serializeMessage(k2::MessageType::ClientRequest, partitionId, MemKVModule<>::RequestWithType(getRequest)));
    if(response->getStatus() != Status::Ok || response->moduleCode != 0)
        std::cerr << "Get failed" << (int)response->getStatus() << std::endl << std::flush;

    MemKVModule<>::GetResponse getResponse;
    response->payload.getReader().read(getResponse);
    std::cout << "Gotten: value: " << getResponse.value << " version: " << getResponse.version << std::endl << std::flush;
}

}

namespace bpo = boost::program_options;

void printHelp(bpo::options_description& desc, std::string& appName)
{
    std::cout << "Usage: " << appName << " (assign|offload|get|set) [options]" << std::endl;
    std::cout << desc << std::endl;
}

int main(int argc, char** argv)
{
    std::string partition, node, command, key, value;
    std::string appName = boost::filesystem::basename(argv[0]);

    bpo::options_description desc("Options");
    desc.add_options()
        ("help,h", "Print help messages")
        ("partition,p", bpo::value<std::string>(&partition)->required(), "Partition assignment id to send message to. Format PartitionId,RangeVersion,AssignmentVersion. E.g. '1.2.3'.")
        ("node,n", bpo::value<std::string>(&node)->default_value("127.0.0.1:11311"), "Node to which partition belongs. 127.0.0.1:11311 by default")
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

        std::cout << "Executing command " << command << " for Node:" << node << " Partition:" << partition << std::endl << std::flush;

        if(!partitionId.parse(partition.c_str()))
        {
            std::cerr << "Cannot parse partition id" << std::endl;
            return 1;
        }
    }
    catch(std::exception& e)
    {
        std::cerr << "Unhandled exception while parsing arguments: " << e.what() << ". " << std::endl;
        printHelp(desc, appName);
        return 1;
    }

    try
    {
        if(command == "assign")
            k2::assignPartition(partitionId, node.c_str());
        else if(command == "offload")
            k2::offloadPartition(partitionId, node.c_str());
        else if(command == "set")
            k2::moduleSet(partitionId, node.c_str(), std::move(key), std::move(value));
        else if(command == "get")
            k2::moduleGet(partitionId, node.c_str(), std::move(key));
        else
        {
            std::cerr << "Unknown command: " << command << std::endl;
            printHelp(desc, appName);
            return 1;
        }

        std::cout << "Command \"" << command << "\" successfully executed."  << std::endl;
    }
    catch(std::exception& e)
    {
        std::cerr << "Unhandled exception while executing command: " << e.what() << ". " << std::endl;
        return 1;
    }
    catch(...)
    {
        std::cerr << "Unhandled exception while executing command" << std::endl;
        return 1;
    }

    return 0;
}
