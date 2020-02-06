#pragma once

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Payload.h"

#include <k2/common/Defer.h>

namespace k2 {
struct fileutil {

// create a directory if it doesn't exist already
static bool makeDir(String path, int mode=0777);

// check to see if a file exists
static bool fileExists(String path);

// read an entire file into a payload
static bool readFile(Payload& payload, String path);

// write an entire payload into the given file
static bool writeFile(Payload&& payload, String path);

}; // struct fileutil
} // ns k2
