/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>

#include "transport/discovery.hpp"
#include "transport/socket.hpp"

namespace corby::client {

const cor_dispatch_v1_t* discover_dispatch() {
    char path[128];
    std::snprintf(
        path,
        sizeof(path),
        "/tmp/corby-%u/process-%ld.provider",
        getuid(),
        static_cast<long>(getpid())
    );
    Fd file(::open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (file.value < 0)
        return nullptr;
    struct stat info{};
    if (::fstat(file.value, &info) || !S_ISREG(info.st_mode) || info.st_uid != getuid() ||
        (info.st_mode & 077))
        return nullptr;
    if (::flock(file.value, LOCK_EX | LOCK_NB) == 0)
        return nullptr; // stale, no provider owns it
    if (errno != EWOULDBLOCK && errno != EAGAIN)
        return nullptr;
    char record[128]{};
    const auto count = ::read(file.value, record, sizeof(record) - 1);
    if (count <= 0)
        return nullptr;
    unsigned long long address = 0;
    long pid = 0;
    if (std::sscanf(record, "CORBY1 %ld %llx", &pid, &address) != 2 || pid != getpid() || !address)
        return nullptr;
    const auto* api = reinterpret_cast<const cor_dispatch_v1_t*>(static_cast<uintptr_t>(address));
    if (api->abi_version != COR_ABI_VERSION || api->size < COR_DISPATCH_V1_REQUIRED_SIZE)
        return nullptr;
    return api;
}

} // namespace corby::client
