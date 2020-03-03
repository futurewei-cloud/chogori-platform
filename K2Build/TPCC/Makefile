# build the image
include K2Build/dfv_k2_build_deps/DockerMake.base
TPCC_IMG_NAME?=tpcc
IMG_NAME=$(TPCC_IMG_NAME)
DEP_IMAGES=$(DOCKER_REGISTRY)/k2base:latest
DOCKERFILE_PATH_ARG=--file K2Build/TPCC/Dockerfile

all: img_build

push: img_push
