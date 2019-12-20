#include <iostream>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <k2/client/PartitionMessageTransport.h>
#include <k2/k2types/PartitionMetadata.h>
#include <k2/common/TimeMeasure.h>

namespace k2
{

constexpr CollectionId collectionId = 3;

void assignPartition(k2::PartitionAssignmentId partitionId, const char* ip, uint16_t port)
{
    AssignmentMessage assignmentMessage;
    assignmentMessage.collectionMetadata = CollectionMetadata(collectionId, ModuleId::Default, {});
    assignmentMessage.partitionMetadata = PartitionMetadata(partitionId.id, PartitionRange("A", "B"), collectionId);   //  TODO: change range
    assignmentMessage.partitionVersion = partitionId.version;

    Stopwatch stopWatch;
    std::unique_ptr<ResponseMessage> response = sendPartitionMessage(ip, port, k2::MessageType::PartitionAssign, partitionId, assignmentMessage);
    std::cout << "assignPartition:" << stopWatch.elapsedUS()  << "us" << std::endl;

    assert(response);
    THROW_IF_BAD(response->getStatus());
}

void offloadPartition(k2::PartitionAssignmentId partitionId, const char* ip, uint16_t port)
{
    (void) partitionId; // TODO use me
    (void) ip; // TODO use me
    (void) port; // TODO use me
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

        std::cout << "Executing command " << command << " for Node:" << nodeIP << ":" << nodePort << " Partition:" << partition << std::endl << std::flush;

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
            k2::assignPartition(partitionId, nodeIP.c_str(), nodePort);
        else if(command == "offload")
            k2::offloadPartition(partitionId, nodeIP.c_str(), nodePort);
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
