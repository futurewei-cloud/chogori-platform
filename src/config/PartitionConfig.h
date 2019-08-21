#pragma once


namespace k2
{
namespace config
{

class PartitionConfig
{
friend class NodePoolConfig;
friend class ConfigParserLegacy;
friend class ConfigParser201907;

struct Range
{
    std::string _upperBound;
    bool _upperBoundClosed = false;
    std::string _lowerBound;
    bool _lowerBoundClosed = false;
}; // struct Range

protected:
    std::string _id;
    Range _range;

public:
    PartitionConfig()
    {
        // empty
    }

    const std::string& getId() const
    {
        return _id;
    }

    const Range& getRange() const
    {
        return _range;
    }


}; // class PartitionConfig

}; // namespace config
}; // namespace k2
