include DFV_K2_BuildDeps/DockerMake.base
BUILD_DIR=build
INSTALL_DIR=$(BUILD_DIR)/install
INSTALL_PFX=/usr/local/
package_version=$(shell git describe --tags `git rev-list --tags --max-count=1`)

all: build

build:
	@echo ">>> Building"
	@mkdir -p ../$(BUILD_DIR)
	@cmake -B../$(BUILD_DIR) -H../ -DCMAKE_INSTALL_PREFIX=../$(INSTALL_DIR)/$(INSTALL_PFX)
	@$(MAKE) -C../$(BUILD_DIR)

test: build
	@echo ">>> Testing"
	@../$(BUILD_DIR)/test/node/node_test && testReturnCode=$? && exit $(testReturnCode)

integrate: build
	@echo ">>> Running integration test"
	@../integration/run.sh && testReturnCode=$? && exit $(testReturnCode)

install: build
	@echo ">>> Installing"
	@$(MAKE) -C../$(BUILD_DIR) install

clean:
	@echo ">>> Cleaning"
	@rm -rf ../$(BUILD_DIR) ../k2platform_*

package: install
	@echo ">>> Packaging"
	@cd ../ && fpm --depends "libseastar-k2 = 1.0.2" -s dir -t deb --deb-generate-changes -n k2platform -f -m k2 -a amd64 -v $(package_version) --deb-dist stable --description "K2 Platform Applications http://rnd-github-usa-g.huawei.com/SeattleCloudBU/DFV_K2_Platform/" -C $(INSTALL_DIR)
	@cd ../ && fpm --depends "libseastar-k2 = 1.0.2" -s dir -t rpm -n k2platform -f -m k2 -a amd64 -v $(package_version) --rpm-dist stable --description "K2 Platform Applications http://rnd-github-usa-g.huawei.com/SeattleCloudBU/DFV_K2_Platform/" -C $(INSTALL_DIR)


builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push
