# build the image
include ../../K2Build/DFV_K2_BuildDeps/DockerMake.base
IMG_NAME=seastar_net_test
DEP_IMAGES=$(DOCKER_REGISTRY)/k2platform_builder:latest


CXXFLAGS=-O2 -mtune=skylake-avx512 -mavx -maes -mbmi -mxsave -mpopcnt -std=gnu++17 -Wno-register -Wno-conversion -U_FORTIFY_SOURCE -fconcepts -DSEASTAR_HAVE_GCC6_CONCEPTS -DSEASTAR_HAVE_DPDK -pthread -I/usr/local/include -I/usr/include/p11-kit-1
DPDK_LIB_DIR = /usr/local/lib
# Even though we set rpath, there are recursive lib dependencies which do
# not use the rpath for some reason, so LD_LIBRARY_PATH will still be
# necessary at runtime
#
# The dpdk design is to load the pmd library at runtime, so programs
# do not need to be recompiled to use different NICs. SeaStar does not 
# support this, so it needs to be included at link time
LDFLAGS += -L$(DPDK_LIB_DIR) -Wl,-rpath,$(abspath $(DPDK_LIB_DIR))
DPDK_SHLIBS := -lrte_ethdev -lrte_net -lrte_mbuf -lrte_mempool -lrte_ring -lrte_mempool_ring \
   -lrte_kvargs -lrte_eal -lrte_pci -lrte_bus_pci -lrte_pmd_mlx5
# -lrte_pmd_i40e
# The no-as-needed flag is needed because dpdk uses gcc extensions to run
# initilization code at runtime. Without the flag this initilization code is
# removed at link time and dpdk will break.
LIBS += -Wl,--no-as-needed $(DPDK_SHLIBS) -Wl,--as-needed
LIBS += -lseastar -lboost_program_options -lboost_system -lboost_thread -lfmt -lcares -lcryptopp
LIBS += $(shell pkg-config seastar --libs-only-l --static)
CXXFLAGS += -I/usr/local/include/dpdk/

all: tcp_client tcp_server

tcp_client.o: tcp_sctp_client_demo.cc Makefile
	$(CXX) $(CXXFLAGS) -o $@ -c tcp_sctp_client_demo.cc

tcp_server.o: tcp_sctp_server_demo.cc Makefile
	$(CXX) $(CXXFLAGS) -o $@ -c tcp_sctp_server_demo.cc

tcp_client: tcp_client.o
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)

tcp_server: tcp_server.o
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)
