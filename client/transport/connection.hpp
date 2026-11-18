/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <atomic>
#include <functional>
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
    uint64_t features = 0;
    using Event = std::function<void(Buffer)>;
    std::mutex events_mutex;
    std::map<std::pair<uint32_t, uint64_t>, Event> events;
    void watch(
        uint32_t opcode,
        uint64_t id,
        Event handler
    );
    void unwatch(
        uint32_t opcode,
        uint64_t id
    );
    void deliver(
        uint32_t opcode,
        uint64_t id,
        Buffer body
    );
    static void failed_event(
        uint32_t opcode,
        const Event& handler
    );
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
