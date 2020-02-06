include dfv_k2_build_deps/DockerMake.base
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

test_plog: build
	@echo ">>> Testing plogmock"
	@../$(BUILD_DIR)/test/plogmock/plogmock_test && testReturnCode=$? && exit $(testReturnCode)

test_volume: build
	@echo ">>> Testing PersistentVolume"
	@../$(BUILD_DIR)/test/persistentVolume/persistent_volume_test && testReturnCode=$? && exit $(testReturnCode)

test_transport: build
	@echo ">>> Testing transport"
	@../$(BUILD_DIR)/test/transport/payload_test && testReturnCode=$? && exit $(testReturnCode)

test: test_plog test_volume test_transport

integrate: build
	@echo ">>> Running integration test"
	@../test/integration/run.sh && testReturnCode=$? && exit $(testReturnCode)

install: build
	@echo ">>> Installing"
	@$(MAKE) -C../$(BUILD_DIR) install

clean:
	@echo ">>> Cleaning"
	@rm -rf ../$(BUILD_DIR) ../k2platform_*

package_deb: install
	@echo ">>> Packaging deb"
	@cd ../ && fpm --depends "libseastar-k2 = 1.0.7" -s dir -t deb --deb-generate-changes -n k2platform -f -m k2 -a amd64 -v $(package_version) --deb-dist stable --description "K2 Platform Applications https://gitlab.futurewei.com/k2/dfv_k2_platform" -C $(INSTALL_DIR)

package_push_deb:
	@$(eval DEB_NAME := $(wildcard ../k2platform_*.deb))
	@$(eval DEB_CH_NAME := $(wildcard ../k2platform_*.changes))
	@echo ">>> Pushing package: $(DEB_NAME)"
	@$(shell curl -s http://k2-bvu-10001.huawei.com:10080/incoming/ --upload-file $(DEB_NAME))
	@echo ">>> Pushing package: $(DEB_CH_NAME)"
	@$(shell curl -s http://k2-bvu-10001.huawei.com:10080/incoming/ --upload-file $(DEB_CH_NAME))

builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push
