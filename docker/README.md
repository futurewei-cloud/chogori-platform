# Building the image
``` sh
docker build -t "chogori-builder" .
```

# Building and installing Chogori-Seastar-RD
## Run the container from the image we built above
This mounts your host current directory `PWD` into the `/host` directory of the container. Any changes you do in the container inside the `/host` directory are reflected into the host (much like a symlink)
``` sh
docker run -it --rm --init -v ${PWD}:/host chogori-builder
```

## Run the chogori-seastar-rd installation steps inside this container
``` sh
cd /host
git clone https://github.com/futurewei-cloud/chogori-seastar-rd.git
cd chogori-seastar-rd
./install-dependencies.sh
./configure.py --mode=release
ninja -C build/release
ninja -C build/release test
ninja -C build/release install
```

## Persisting the container-local changes
do not exit the container from above just yet. Instead, go to another shell and run the command:
``` sh
$ docker container ls
CONTAINER ID   IMAGE             COMMAND       CREATED         STATUS         PORTS     NAMES
b34f8a36a25b   chogori-builder   "/bin/bash"   6 minutes ago   Up 6 minutes             determined_wilson
$ docker commit b34f8a36a25b builder-with-seastar
```

Now you can exit the container in your original session.

# Building and installing Chogori-Platform
## Run the container with the installed seastar
``` sh
docker run -it --rm --init -v ${PWD}:/host builder-with-seastar
```

## And now run the platform build and installation steps inside this container
``` sh
cd /host
git clone https://github.com/futurewei-cloud/chogori-platform.git
cd chogori-platform
./install_deps.sh
mkdir build
cd build
cmake ../
make -j
cd test
ctest
cd ../
make install
```
