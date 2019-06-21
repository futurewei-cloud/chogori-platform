# build the image
ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/..
MK_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include ../K2Build/DFV_K2_BuildDeps/DockerMake.base
IMG_NAME=k2nodepoolrunner
DEP_IMAGES=$(DOCKER_REGISTRY)/k2base:latest

all: img_build

push: img_push

img_build:
	@echo ">>> Pulling dependent images: $(DEP_IMAGES)"
	@for dep in $(DEP_IMAGES) ; do \
		docker pull $$dep ; \
	done

	@echo ">>> Building image $(IMG_NAME)"
	@echo "root is $(ROOT_DIR)"

	echo "Building docker image: $(IMG_NAME)"
	docker build $(NO_CACHE) $(DOCKER_ARGS) -t $(IMG_NAME) --network host -f $(MK_DIR)/Dockerfile $(MK_DIR)
	docker tag $(IMG_NAME):latest $(DOCKER_REGISTRY)/$(IMG_NAME)

