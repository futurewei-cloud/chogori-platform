//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include <k2/persistence/service/PersistenceService.h>

int main(int argc, char** argv) {
    k2::App app;
    // pass the ss::distributed container to the PersistenceService constructor
    app.addApplet<k2::PersistenceService>();
    return app.start(argc, argv);
}
