/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "corby/dispatch.h"
#include "transport/protocol.hpp"

namespace corby::client {

// Child handles share the connection so context destruction cannot end their session.
struct Connection {
    const cor_dispatch_v1_t* api = nullptr;
    cor_context_t* native = nullptr;
    std::string endpoint;
    int control = -1;
    Bytes token;
    std::atomic<bool> alive{true};
    std::thread reader;
    std::mutex flags_mutex;
    std::map<uint64_t, std::shared_ptr<std::atomic<uint32_t>>> flags;

    ~Connection();
    void fail() noexcept;
    std::shared_ptr<std::atomic<uint32_t>> flag(uint64_t id);
    void notifications() noexcept;
    cor_status_t rpc(
        uint32_t opcode,
        Buffer arguments,
        Buffer& response
    );
};

} // namespace corby::client
