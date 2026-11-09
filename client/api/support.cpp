/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <algorithm>
#include <cstring>

#include "api/support.hpp"

namespace corby::client {

cor_status_t prefix(
    const void* value,
    size_t minimum,
    uint32_t& size
) {
    if (!value)
        return COR_INVALID_ARGUMENT;
    std::memcpy(&size, value, 4);
    if (size < minimum)
        return COR_INVALID_ARGUMENT;
    uint32_t abi;
    std::memcpy(&abi, static_cast<const uint8_t*>(value) + 4, 4);
    return abi == COR_ABI_VERSION ? COR_OK : COR_UNSUPPORTED;
}
cor_status_t copy_stats(
    Buffer& body,
    cor_engine_stats_t* out
) {
    uint32_t size;
    const auto status = prefix(out, 8, size);
    if (status)
        return status;
    cor_engine_stats_t value{};
    value.size = static_cast<uint32_t>(body.get(4));
    value.abi_version = static_cast<uint32_t>(body.get(4));
    value.active_work = body.get(8);
    value.grants = body.get(8);
    value.held_ns = body.get(8);
    value.waited_ns = body.get(8);
    value.waiting = static_cast<uint32_t>(body.get(4));
    value.holding = static_cast<uint32_t>(body.get(4));
    value.weight = static_cast<uint32_t>(body.get(4));
    value.tenancy = static_cast<uint32_t>(body.get(4));
    value.capabilities = body.get(8);
    std::memcpy(out, &value, std::min<size_t>(size, sizeof(value)));
    return COR_OK;
}
cor_status_t copy_label(
    Buffer& body,
    char* out,
    uint32_t capacity,
    uint32_t* needed
) {
    if (!needed)
        return COR_INVALID_ARGUMENT;
    const auto length = static_cast<uint32_t>(body.get(4));
    if (length > body.bytes.size() - body.offset)
        return COR_INTERNAL;
    *needed = length + 1;
    if (!out)
        return capacity == 0 ? COR_OK : COR_INVALID_ARGUMENT;
    if (capacity < *needed)
        return COR_INVALID_ARGUMENT;
    std::memcpy(out, body.bytes.data() + body.offset, length);
    out[length] = 0;
    return COR_OK;
}

} // namespace corby::client
