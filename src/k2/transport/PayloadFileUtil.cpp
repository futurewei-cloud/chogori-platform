#include "PayloadFileUtil.h"

namespace k2 {

// create a directory if it doesn't exist already
bool fileutil::makeDir(String path, int mode) {
    if (::mkdir(path.c_str(), mode) != 0) {
        if (errno != EEXIST) {
            K2ERROR("Unable to create directory " << path << ": " << strerror(errno));
            return false;
        }
    }
    return true;
}

// check to see if a file exists
bool fileutil::fileExists(String path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno != ENOENT) {
            K2ERROR("problem reading file: name=" << path << ":: " << strerror(errno));
        }
        return false;
    }
    ::close(fd);
    return true;
}

// read an entire file into a payload
bool fileutil::readFile(Payload& payload, String path) {
    if (!fileExists(path)) return false;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        K2ERROR("error openning file: " << path << " -->" << strerror(errno));
        return false;
    }

    payload.clear();
    bool success = false;

    Defer d([&] {
        if (!success) {
            payload.clear();
        }
        ::close(fd);
    });

    while (1) {
        Binary buf(4096);
        auto rd = ::read(fd, buf.get_write(), buf.size());
        if (rd < 0) {
            K2ERROR("problem reading file: " << strerror(errno));
            success = false;
            break;
        }
        if (rd == 0) {
            success = true;
            break;
        }
        buf.trim(rd);
        payload.appendBinary(std::move(buf));
    }
    return success;
}

// write an entire payload into the given file
bool fileutil::writeFile(Payload&& payload, String path) {
    payload.truncateToCurrent();
    auto leftBytes = payload.getSize();

    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY);
    if (fd < 0) {
        K2ERROR("Unable to open file for writing: name=" << path << ", err=" << strerror(errno));
        return false;
    }

    Defer d([&] {
        ::close(fd);
    });

    for (auto&& buf : payload.release()) {
        auto towrite = std::min(buf.size(), leftBytes);
        size_t written = ::write(fd, buf.get(), towrite);
        if (written != towrite) {
            return false;
        }
        leftBytes -= written;
        if (leftBytes == 0) break;
    }
    return true;
}

} // ns k2
