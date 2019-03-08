include DFV_K2_BuildDeps/DockerMake.base

package_version=$(shell git describe --tags `git rev-list --tags --max-count=1`)

all: build
# replace with module
BUILD_DIR=build
build:
	@echo ">>> Building"
	@cd ../ && mkdir -p ${BUILD_DIR} && cd ${BUILD_DIR} && cmake ..
	@cd ../${BUILD_DIR}/ && $(MAKE)

clean:
	@echo ">>> Cleaning"
	@cd ../ && rm -rf $(BUILD_DIR)

builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push
