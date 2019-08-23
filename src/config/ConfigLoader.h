#pragma once
// std
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
// k2
#include <common/Common.h>
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
        YAML::Node node = YAML::LoadFile(configFile);

        return loadConfig(node);
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
            pParser = std::move(std::make_unique<ConfigParser201907>());
        }
        else {
            pParser = std::move(std::make_unique<ConfigParserLegacy>());
        }

        return pParser->parseConfig(node);
    }

    static std::vector<std::shared_ptr<NodePoolConfig>> getHostNodePools(std::shared_ptr<Config> pConfig)
    {
        return std::move(pConfig->getNodePoolsForHost(getHostname()));
    }

    static std::string getHostname()
    {
        const size_t masSize = 512;
        char hostname[masSize];
        const int result = gethostname(hostname, masSize);
        ASSERT(!result);

        return std::move(std::string(hostname));
    }

}; // class ConfigLoader

}; // namespace config
}; // namespace k2
