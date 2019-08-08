#pragma once

// k2:config
#include "YamlUtils.h"
#include "IConfigParser.h"
#include "ConfigParserLegacy.h"

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
        std::string schema = YamlUtils::getOptionalValue(node["schema"], std::string("legacy"));
        std::unique_ptr<IConfigParser> pParser = std::make_unique<ConfigParserLegacy>();
        std::shared_ptr<Config> pConfig = std::make_shared<Config>();
        if(schema=="legacy") {
            pConfig = pParser->parseConfig(node);
        }

        return pConfig;
    }

    static std::shared_ptr<Config> loadDefaultConfig()
    {
        YAML::Node node;
        std::string schema = YamlUtils::getOptionalValue(node["schema"], std::string("legacy"));
        std::unique_ptr<IConfigParser> pParser = std::make_unique<ConfigParserLegacy>();
        std::shared_ptr<Config> pConfig = std::make_shared<Config>();
        if(schema=="legacy") {
            pConfig = pParser->parseConfig(node);
        }

        return pConfig;
    }

}; // class ConfigLoader

}; // namespace config
}; // namespace k2
