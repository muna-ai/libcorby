/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <chrono>
#include <memory>

#include "api/callbacks.hpp"
#include "transport/connection.hpp"

namespace corby::client {
using Clock = std::chrono::steady_clock;

struct Handle {
    std::shared_ptr<Connection> client;
    uint64_t id = 0;
};

template <typename T>
T* native(uint64_t id) {
    return reinterpret_cast<T*>(static_cast<uintptr_t>(id));
}
} // namespace corby::client

// Definitions of the opaque public C handles; never exposed to consumers.
struct cor_context {
    std::shared_ptr<corby::client::Connection> client;
};
struct cor_engine : corby::client::Handle {
    std::shared_ptr<corby::client::Callbacks> callbacks = std::make_shared<corby::client::Callbacks>();
};
struct cor_work : corby::client::Handle { uint32_t resource = COR_RESOURCE_COMPUTE; };
struct cor_lease : corby::client::Handle {
    std::shared_ptr<std::atomic<uint32_t>> flags;
    corby::client::Clock::time_point started;
    uint32_t resource = COR_RESOURCE_COMPUTE;
    std::shared_ptr<corby::client::Callbacks> callbacks = std::make_shared<corby::client::Callbacks>();
};

struct cor_reclaim : corby::client::Handle {};
