include DFV_K2_BuildDeps/DockerMake.base

package_version=$(shell git describe --tags `git rev-list --tags --max-count=1`)

all: build
# replace with module
SEASTAR_DIR=${PWD}/../../DFV_K2_SeaStar
APP_DIR=${PWD}/../
BUILD_DIR=build
build:
	@echo ">>> Building"
	@echo ${SEASTAR_DIR}
	@cd ${PWD}/../${BUILD_DIR}/ && cmake -DCMAKE_PREFIX_PATH="${SEASTAR_DIR}/build/release;${SEASTAR_DIR}/build/release/_cooking/installed" -DCMAKE_MODULE_PATH=${SEASTAR_DIR}/cmake ${APP_DIR}
	@cd ${PWD}/../${BUILD_DIR}/ && $(MAKE)

builder_img_build:
	@$(MAKE) -C Builder

builder_img_push:
	@$(MAKE) -C Builder -s push
