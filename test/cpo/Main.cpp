//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include "CPOTest.h"

int main(int argc, char** argv) {
    k2::App app;
    app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO service");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
    app.addApplet<CPOTest>();
    return app.start(argc, argv);
}
