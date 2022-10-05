Before we can build the platform, we need an environment with all dependencies pre-installed. These instructions help you create this environment using Docker.
# Preparation

## Building the image
assuming you checked-out chogori-platform under some folder, say `workspace`
``` sh
cd workspace/chogori-platform/docker
docker build -t "chogori-builder" .
```

## Building and installing Chogori-Seastar-RD
This is a fundamental dependency for the project and has to be installed in order to build and run the platform.
### Clone the repo
``` sh
cd workspace && git clone https://github.com/futurewei-cloud/chogori-seastar-rd.git
```
### Run the container from the image we built above
This mounts your host current directory `PWD` into the `/host` directory of the container. Any changes you do in the container inside the `/host` directory are reflected into the host (much like a symlink)
``` sh
cd workspace/chogori-seastar-rd
docker run -it --rm --init -v ${PWD}:/host chogori-builder
```

### Run the chogori-seastar-rd installation steps inside this container
``` sh
cd /host
./install-dependencies.sh
./configure.py --mode=release
ninja -C build/release install
```

# Building and installing Chogori-Platform
We're now ready to build the platform


## And now run the platform build and installation steps inside this container
``` sh
cd /host
./install_deps.sh
```
## Persisting the container-local changes
at this time, we have a running container with the seastar-rd dependency installed. We should now save the container state so that we can restart it if we want with the dependency already pre-installed.
Do not exit the container from above just yet. Instead, go to another shell and run the command:
``` sh
$ docker container ls
CONTAINER ID   IMAGE             COMMAND       CREATED         STATUS         PORTS     NAMES
b34f8a36a25b   chogori-builder   "/bin/bash"   6 minutes ago   Up 6 minutes             determined_wilson
$ docker commit b34f8a36a25b chogori-builder
```
This will overwrite the container with the latest state. The actual ID would be different for you as it is randomly generated.
Now you can exit the container in your original session if you'd like

## Building the platform code
``` sh
mkdir build && cd build && cmake ../ && make -j && cd -
```

To run the tests and install:
``` sh
cd test
ctest
cd ../
make install
```
