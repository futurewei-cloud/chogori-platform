//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include <k2/assignmentManager/AssignmentManager.h>
#include <k2/collectionMetadataCache/CollectionMetadataCache.h>
#include <k2/nodePoolMonitor/NodePoolMonitor.h>
#include <k2/partitionManager/PartitionManager.h>

int main(int argc, char** argv) {
    k2::App app;

    app.addOptions()
        ("k23si_cpo_endpoint", bpo::value<k2::String>(), "the endpoint for k2 CPO service")
        ("k23si_persistence_endpoint", bpo::value<k2::String>(), "the endpoint for k2 persistence");

    app.addApplet<k2::AssignmentManager>();
    app.addApplet<k2::NodePoolMonitor>();
    app.addApplet<k2::PartitionManager>();
    app.addApplet<k2::CollectionMetadataCache>();

    return app.start(argc, argv);
}
