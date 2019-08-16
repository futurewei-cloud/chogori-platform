#define CATCH_CONFIG_MAIN
// std
#include <vector>
// catch
#include "catch2/catch.hpp"
// k2
#include <config/Config.h>
#include <config/ConfigLoader.h>

using namespace k2;
using namespace k2::config;


SCENARIO("Config", "[2019-07]")
{
    WHEN("2019-07 config")
    {
        const std::string configStr =
        {R"(---
            schema: "2019-07"
            instance-version: 123456
            cluster-name: "mysql-backend-na"

            transport-base: &transport-base
                rdma:
                    nic_id: "mlx5_0"
                tcp:
                    address: "0.0.0.0"
                    port: 1000

            node-pool-base: &node-pool-base
                node-count: 2
                enable-monitoring: false
                transport:
                    <<: *transport-base

            node-base: &node-base
                transport:
                    <<: *transport-base
                    tcp:
                        port: 2000

            node-pools:
                -   id: "np1"
                    <<: *node-pool-base
                    node-count: 3
                    nodes:
                        -   id: 0
                            <<: *node-base
                            cpu: 15
                        -   id: 1
                            <<: *node-base
                            transport:
                                tcp:
                                    port: 3000

                -   id: "np2"
                    <<: *node-pool-base

            cluster:
                -   host: "hostname1"
                    node-pools:
                        -   id: "np1"
                            transport:
                                tcp:
                                    address: "192.168.200.1"
                            nodes:
                                -   id: 0
                                    partitions: [ "1.1.1", "2.1.1" ]
                                    transport:
                                        rdma:
                                            address: "host1.local"
                                            port: 246

        )"};

        THEN("Parse configuration")
        {
            auto pConfig = ConfigLoader::loadConfigString(configStr);
            REQUIRE(pConfig->getSchema() == "2019-07");

            auto nodePools = pConfig->getNodePools();
            REQUIRE(nodePools.size() == 2);

            REQUIRE(nodePools[0]->getId() == "np1");
            REQUIRE(nodePools[0]->getNodes().size() == 3);
            REQUIRE(nodePools[0]->getNodes()[0]->getTransport()->getTcpPort() == 2000);
            REQUIRE(nodePools[0]->getNodes()[1]->getTransport()->getTcpPort() == 3000);
            REQUIRE(nodePools[0]->getNodes()[2]->getTransport()->getTcpPort() == 1000);
            REQUIRE(nodePools[0]->getTransport()->getTcpAddress() == "192.168.200.1");
            REQUIRE(nodePools[0]->getNodes()[0]->getTransport()->getRdmaPort() == 246);
            REQUIRE(nodePools[0]->getNodes()[0]->getTransport()->getRdmaAddress() == "host1.local");
            REQUIRE(nodePools[0]->getNodes()[0]->getPartitions().size() == 2);
            REQUIRE(nodePools[0]->getNodes()[0]->getPartitions()[0] == "1.1.1");

            REQUIRE(nodePools[1]->getId() == "np2");
            REQUIRE(nodePools[1]->getNodes().size() == 2);
            REQUIRE(nodePools[1]->getTransport()->getTcpAddress() == "0.0.0.0");
        }
    }

    WHEN("legacy config")
    {
        const std::string configStr =
        {R"(---
            nodes_count: 2
            nodes_cpu_set: "30"
            pool_cpu_set: "18,19"
            address: "192.168.1.1"
            nodes_minimum_port: 11312
            monitorEnabled: true
            rdmaEnabled: true
            hugepages: true
            memory: "10G"
            nic_id: "mlx5_0"
        )"};

        THEN("Parse configuration")
        {
            auto pConfig = ConfigLoader::loadConfigString(configStr);
            REQUIRE(pConfig->getSchema() == "legacy");

            auto nodePools = pConfig->getNodePools();
            REQUIRE(nodePools.size() == 1);
            REQUIRE(nodePools[0]->isMonitoringEnabled() == true);
            REQUIRE(nodePools[0]->isHugePagesEnabled() == true);
            REQUIRE(nodePools[0]->getMemorySize() == "10G");
            REQUIRE(nodePools[0]->getNodes().size() == 2);
            REQUIRE(nodePools[0]->getTransport()->isRdmaEnabled() == true);
            REQUIRE(nodePools[0]->getTransport()->getRdmaNicId() == "mlx5_0");
            REQUIRE(nodePools[0]->getTransport()->getTcpAddress() == "192.168.1.1");
            REQUIRE(nodePools[0]->getTransport()->getTcpPort() == 11312);
            REQUIRE(nodePools[0]->getNodes()[0]->getTransport()->getTcpPort() == 11312);
            REQUIRE(nodePools[0]->getNodes()[1]->getTransport()->getTcpPort() == 11313);
        }
    }
}
