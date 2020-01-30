#pragma once

// k2
#include <k2/common/Common.h>

// k2:config
#include "NodePoolConfig.h"
#include "PartitionManagerConfig.h"
#include "ClusterConfig.h"

// third-party
#include <boost/program_options.hpp>
#include <boost/spirit/include/qi.hpp>
#include <seastar/core/distributed.hh>  // for distributed<>

namespace k2 {
namespace config {
typedef boost::program_options::variables_map BPOVarMap;
typedef seastar::distributed<BPOVarMap> BPOConfigMapDist_t;
}  // ns config

// for convenient access to globally initialized configuration
extern config::BPOConfigMapDist_t ___config___;
inline config::BPOConfigMapDist_t& ConfigDist() { return ___config___; }
inline const config::BPOVarMap& Config() { return ___config___.local(); }

// Helper class used to read configuration values in code.
// To use, declare a variable for your configuration, e.g.:
// ConfigVar<int> retries("retries", 10);
//
// Then later in the code when you want to read the configured value, just use the variable as a functor
// for(int i = 0; i < retries() << ++i) {
// }
template<typename T>
class ConfigVar {
public:
    ConfigVar(String name, T defaultValue=T{}) {
        if (Config().count(name)) {
            _val = Config()[name].as<T>();
        }
        else {
            _val = std::move(defaultValue);
        }
    }
    ~ConfigVar(){}
    const T& operator()() const {
        return _val;
    }
private:
    T _val;
};

// This class is parseable via BoostProgramOptions to allow users to accept human-readable (think chrono literals)
// durations, e.g. 1ms, 1us, 21h
// The code which adds the config option should add an option of type ParseableDuration
// Then to read these, use the ConfigDuration class below
struct ParseableDuration {
    Duration value;
};

template <class charT>
void validate(boost::any& v, const std::vector<std::basic_string<charT>>& xs, ParseableDuration*, long) {
    boost::program_options::validators::check_first_occurrence(v);
    std::basic_string<charT> s(boost::program_options::validators::get_single_string(xs));

    int magnitude;
    Duration factor;

    namespace qi = boost::spirit::qi;
    qi::symbols<char, Duration> unit;
    unit.add("ns", 1ns)("us", 1us)("µs", 1us)("ms", 1ms)("s", 1s)("m", 1min)("h", 1h);

    if (parse(s.begin(), s.end(), qi::int_ >> unit >> qi::eoi, magnitude, factor))
        v = ParseableDuration{magnitude * factor};
    else
        throw boost::program_options::invalid_option_value(s);
}

// Allows for easy reading of Duration variables. The user registers a ParsedDuration in the program options
// then they create a ConfigDuration variable. e.g.
// ConfigDuration _timeout("timeout", 10ms);
//...
// if (clock.now() - start > _timeout()) throw std::runtime_error("timeout has occurred");
class ConfigDuration: public ConfigVar<ParseableDuration> {
public:
    ConfigDuration(String name, Duration defaultDuration) :
        ConfigVar(std::move(name), ParseableDuration{defaultDuration}) {
    }
    virtual ~ConfigDuration() {}
    const Duration& operator()() const {
        return ConfigVar::operator()().value;
    }
};

namespace config
{

class Config
{
friend class ConfigParserLegacy;
friend class ConfigParser201907;

protected:
    uint64_t _instanceVersion;
    std::string _schema;
    std::string _clusterName;
    std::map<std::string, std::shared_ptr<NodePoolConfig>> _nodePoolMap;
    std::map<std::string, std::vector<std::shared_ptr<NodePoolConfig>>> _clusterMap;
    std::shared_ptr<PartitionManagerConfig> _pPartitionManager;
    std::shared_ptr<NodePoolConfig> _pClientNodePoolConfig;

public:
    Config()
    : _pClientNodePoolConfig(std::make_shared<NodePoolConfig>())
    {
        // empty
    }

    std::vector<std::shared_ptr<NodePoolConfig>> getNodePools() const
    {
        std::vector<std::shared_ptr<NodePoolConfig>> nodePools;
        std::transform(
            std::begin(_nodePoolMap),
            std::end(_nodePoolMap),
            std::back_inserter(nodePools),
            [](const auto& pair) {
                return pair.second;
            }
        );

        return nodePools;
    }

    const std::shared_ptr<NodePoolConfig> getNodePool(const std::string& id) const
    {
        auto pair = _nodePoolMap.find(id);

        return (pair!=_nodePoolMap.end()) ? pair->second : nullptr;
    }

    std::vector<std::shared_ptr<PartitionConfig>> getPartitions() const
    {
        std::vector<std::shared_ptr<PartitionConfig>> partitions;

        for(auto pNodePoolConfig : getNodePools()) {
            for(auto pNodeConfig : pNodePoolConfig->getNodes()) {
                auto vector = std::move(pNodeConfig->getPartitions());
                partitions.insert(partitions.end(), vector.begin(), vector.end());
            }
        }

        return partitions;
    }

    std::vector<std::shared_ptr<NodeConfig>> getClusterNodes() const
    {
        std::vector<std::shared_ptr<NodeConfig>> configs;

        for(auto pair : _clusterMap) {
            for(auto pNodePoolConfig : pair.second) {
                for(auto pNodeConfig : pNodePoolConfig->getNodes()) {
                    configs.push_back(pNodeConfig);
                }
            }
        }

        return configs;
    }

    std::vector<std::shared_ptr<NodePoolConfig>> getNodePoolsForHost(const std::string& hostname)
    {
        auto it = _clusterMap.find(hostname);
        if(it==_clusterMap.end()) {
            return std::vector<std::shared_ptr<NodePoolConfig>>();
        }

        return it->second;
    }

    const std::shared_ptr<PartitionManagerConfig> getPartitionManager() const
    {
        return _pPartitionManager;
    }

    const std::shared_ptr<NodePoolConfig> getClientConfig() const
    {
        return _pClientNodePoolConfig;
    }

    const std::string& getSchema() const
    {
        return _schema;
    }

    const std::string& getClusterName() const
    {
        return _clusterName;
    }

    uint64_t getInstanceVersion() const
    {
        return _instanceVersion;
    }

}; // class Config
} // namespace config
} // namespace k2
