//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include <k2/cpo/CPOService.h>

int main(int argc, char** argv) {
    k2::App app;
    app.addOptions()
        ("data_dir", bpo::value<std::string>(), "The directory where we can keep data")
        ("cluster_nodes", bpo::value<std::vector<std::string>>()->multitoken(), "A list(space-delimited) of cluster node endpoints available for management");
    app.addApplet<k2::CPOService>([&]() mutable -> seastar::distributed<k2::CPOService>& {
        return app.getDist<k2::CPOService>();
    });
    return app.start(argc, argv);
}
