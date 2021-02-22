#include <k2/common/Common.h>

namespace k2 {

inline String getHostName() {
    static const long max_hostname = sysconf(_SC_HOST_NAME_MAX);
    static const long size = (max_hostname > 255) ? max_hostname + 1 : 256;
    String hostname(size, '\0');
    ::gethostname(hostname.data(), size - 1);
    hostname.resize(strlen(hostname.c_str()));
    return hostname;
}

} // ns k2
