/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <cstring>
#include <utility>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "transport/socket.hpp"

namespace corby::client {

int connect_socket(const std::string& path) {
    sockaddr_un address{};
    if (path.empty() || path.size() >= sizeof(address.sun_path))
        return -1;
    Fd socket(::socket(AF_UNIX, SOCK_STREAM, 0));
    if (socket.value < 0)
        return -1;
    ::fcntl(socket.value, F_SETFD, FD_CLOEXEC);
#ifdef SO_NOSIGPIPE
    int one = 1;
    ::setsockopt(socket.value, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
    if (::connect(socket.value, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
        return -1;
    return std::exchange(socket.value, -1);
}

} // namespace corby::client
