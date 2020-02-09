#pragma once
#include <k2/dto/Collection.h>
#include <k2/appbase/AppEssentials.h>

namespace k2 {

class K23SIPartitionModule {
public:
    K23SIPartitionModule(const String& cname, dto::Partition partition);
    ~K23SIPartitionModule();

    seastar::future<> stop();
private:
    String _cname;
    dto::Partition _partition;
};

} // ns k2
