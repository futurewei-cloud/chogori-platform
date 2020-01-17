add_executable (cpo_test Main.cpp CPOTest.cpp CPOTest.h)

target_link_libraries (cpo_test PRIVATE k2appbase Seastar::seastar k2dto)
