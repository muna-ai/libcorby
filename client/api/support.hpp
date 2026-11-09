/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <stdexcept>

#include "corby/corby.h"
#include "transport/protocol.hpp"

namespace corby::client {

template <typename F>
cor_status_t protect(F&& f) noexcept {
    try {
        return f();
    } catch (const std::invalid_argument&) {
        return COR_INVALID_ARGUMENT;
    } catch (...) {
        return COR_INTERNAL;
    }
}

cor_status_t prefix(
    const void* value,
    size_t minimum,
    uint32_t& size
);
cor_status_t copy_stats(
    Buffer& body,
    cor_engine_stats_t* out
);
cor_status_t copy_label(
    Buffer& body,
    char* out,
    uint32_t capacity,
    uint32_t* needed
);

} // namespace corby::client
