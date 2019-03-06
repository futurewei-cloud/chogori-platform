include DFV_K2_BuildDeps/DockerMake.base

package_version=$(shell git describe --tags `git rev-list --tags --max-count=1`)

all: build
INSTALL_PFX=install/usr/local
BUILD_DIR=build/release
build: configure
	@echo ">>> Building"
	@cd ../ && ninja -C $(BUILD_DIR)

configure: submodule
	@echo ">>> Configuring"
	@cd ../ && ./configure.py --enable-dpdk --mode=release --prefix=$(INSTALL_PFX)

submodule:
	@echo ">>> Fetching submodules"
	@cd ../ && git submodule update --init

test: build
	@echo ">>> Testing"
	@cd ../ && ninja -C $(BUILD_DIR) test

install: build
	@echo ">>> Installing"
	@cd ../ && ninja -C $(BUILD_DIR) install

clean:
	@echo ">>> Cleaning"
	@cd ../ && ninja -C $(BUILD_DIR) clean
	@rm -f libseastar-k2_* || true

package: install
	@echo ">>> Packaging"
	@cd ../ && fpm --depends libfmt-k2 --depends libdpdk-k2-dev -s dir -t deb --deb-generate-changes -n libseastar-k2 -f -m k2 -a amd64 -v $(package_version) --deb-dist stable --description "Seastar k2 package http://rnd-github-usa-g.huawei.com/SeattleCloudBU/DFV_K2_SeaStar/" -C $(BUILD_DIR)/install

builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push

