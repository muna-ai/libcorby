/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace corby::client {

using Bytes = std::vector<uint8_t>;

struct Buffer {
    Bytes bytes;
    size_t offset = 0;

    void u32(uint32_t value);
    void u64(uint64_t value);
    void string(const char* value);
    uint64_t get(size_t count);
};

bool send_frame(
    int fd,
    uint32_t opcode,
    const Bytes& body
);
bool read_any_frame(
    int fd,
    uint32_t& opcode,
    Buffer& out
);
bool read_frame(
    int fd,
    uint32_t opcode,
    Buffer& out
);

} // namespace corby::client
