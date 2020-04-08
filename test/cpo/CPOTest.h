#pragma once
#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>

class CPOTest {
public:  // application lifespan
    CPOTest();
    ~CPOTest();

    // required for seastar::distributed interface
    seastar::future<> stop();
    seastar::future<> start();

    seastar::future<> runTest1();
    seastar::future<> runTest2();
    seastar::future<> runTest3();
    seastar::future<> runTest4();
    seastar::future<> runTest5();

private:
    int exitcode = -1;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    k2::ConfigVar<std::vector<k2::String>> _k2ConfigEps{"k2_endpoints"};
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;
};
