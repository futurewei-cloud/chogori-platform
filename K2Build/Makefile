include DFV_K2_BuildDeps/DockerMake.base
BUILD_DIR=../build
INSTALL_PFX=${BUILD_DIR}/install/usr/local/
package_version=$(shell git describe --tags `git rev-list --tags --max-count=1`)

all: build

build:
	@echo ">>> Building"
	@mkdir -p ${BUILD_DIR}
	@cmake -B$(BUILD_DIR) -H../ -DCMAKE_INSTALL_PREFIX=${INSTALL_PFX}
	@$(MAKE) -C${BUILD_DIR}

install: build
	@echo ">>> Installing"
	@$(MAKE) -C${BUILD_DIR} install

clean:
	@echo ">>> Cleaning"
	@rm -rf $(BUILD_DIR)

builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push
