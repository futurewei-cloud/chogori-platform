#pragma once
// std
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string>
#include <streambuf>
#include <fstream>
#include <iostream>
#include <sstream>
// k2
#include <k2/common/Common.h>
// k2:config
#include "YamlUtils.h"
#include "IConfigParser.h"
#include "ConfigParserLegacy.h"
#include "ConfigParser201907.h"

namespace k2
{
namespace config
{

class ConfigLoader
{

public:
    static std::shared_ptr<Config> loadConfig(const std::string& configFile)
    {
        std::ifstream inFile;
        inFile.open(configFile);
        std::stringstream strStream;
        strStream << inFile.rdbuf();
        std::string str = strStream.str();

        return loadConfigString(str);
    }

    static std::shared_ptr<Config> loadDefaultConfig()
    {
        YAML::Node node;

        return loadConfig(node);
    }

    static std::shared_ptr<Config> loadConfigString(const std::string& configString)
    {
        return loadConfig(YAML::Load(configString.c_str()));
    }

    static std::shared_ptr<Config> loadConfig(const YAML::Node& node)
    {
        std::string schema = YamlUtils::getOptionalValue(node["schema"], std::string("legacy"));
        std::unique_ptr<IConfigParser> pParser = std::make_unique<ConfigParserLegacy>();
        std::shared_ptr<Config> pConfig;
        if(schema=="2019-07") {
            pParser = std::make_unique<ConfigParser201907>();
        }

        return pParser->parseConfig(node);
    }

    static std::vector<std::shared_ptr<NodePoolConfig>> getHostNodePools(std::shared_ptr<Config> pConfig)
    {
        return pConfig->getNodePoolsForHost(getHostname());
    }

    static std::string getHostname()
    {
        const size_t masSize = 512;
        char hostname[masSize];
        const int result = gethostname(hostname, masSize);
        assert(!result);

        return hostname;
    }

}; // class ConfigLoader

}; // namespace config
}; // namespace k2
