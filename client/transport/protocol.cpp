/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>

#include "transport/protocol.hpp"

namespace corby::client {

namespace {
constexpr uint32_t MAGIC = 0x434f5242;
constexpr size_t LIMIT = 65536;
bool transfer(
    int fd,
    void* data,
    size_t size,
    bool writing
) {
    auto* bytes = static_cast<uint8_t*>(data);
    while (size) {
#if defined(MSG_NOSIGNAL)
        const auto count =
            writing ? ::send(fd, bytes, size, MSG_NOSIGNAL) : ::recv(fd, bytes, size, 0);
#else
        const auto count = writing ? ::send(fd, bytes, size, 0) : ::recv(fd, bytes, size, 0);
#endif
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
            return false;
        bytes += count;
        size -= static_cast<size_t>(count);
    }
    return true;
}
} // namespace

void Buffer::u32(uint32_t x) {
    for (uint32_t i = 0; i < 4; ++i)
        bytes.push_back(static_cast<uint8_t>(x >> (8 * i)));
}

void Buffer::u64(uint64_t x) {
    for (uint32_t i = 0; i < 8; ++i)
        bytes.push_back(static_cast<uint8_t>(x >> (8 * i)));
}

void Buffer::string(const char* value) {
    const auto length = strnlen(value, 4097);
    if (length > 4096)
        throw std::invalid_argument("label too long");
    u32(static_cast<uint32_t>(length));
    bytes.insert(bytes.end(), value, value + length);
}

uint64_t Buffer::get(size_t count) {
    if (count > bytes.size() - offset)
        throw std::invalid_argument("short reply");
    uint64_t value = 0;
    for (size_t i = 0; i < count; ++i)
        value |= uint64_t(bytes[offset++]) << (8 * i);
    return value;
}

bool send_frame(
    int fd,
    uint32_t opcode,
    const Bytes& body
) {
    if (body.size() > LIMIT)
        return false;
    Buffer frame;
    frame.u32(MAGIC);
    frame.u32(1);
    frame.u32(opcode);
    frame.u32(static_cast<uint32_t>(body.size()));
    frame.bytes.insert(frame.bytes.end(), body.begin(), body.end());
    return transfer(fd, frame.bytes.data(), frame.bytes.size(), true);
}
bool read_frame(
    int fd,
    uint32_t opcode,
    Buffer& out
) {
    Buffer header;
    header.bytes.resize(16);
    if (!transfer(fd, header.bytes.data(), 16, false))
        return false;
    if (header.get(4) != MAGIC || header.get(4) != 1 || header.get(4) != opcode)
        return false;
    const auto length = header.get(4);
    if (length > LIMIT)
        return false;
    out.bytes.resize(length);
    out.offset = 0;
    return transfer(fd, out.bytes.data(), out.bytes.size(), false);
}

} // namespace corby::client
