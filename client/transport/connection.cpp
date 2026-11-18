/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <sys/socket.h>

#include "transport/connection.hpp"
#include "transport/socket.hpp"

namespace corby::client {

Connection::~Connection() {
    if (control >= 0) {
        ::shutdown(control, SHUT_RDWR);
        if (reader.joinable())
            reader.join();
        ::close(control);
    }
    if (api && native)
        (void)api->context_destroy(native);
}

void Connection::fail() noexcept {
    if (!alive.exchange(false))
        return;
    if (control >= 0)
        ::shutdown(control, SHUT_RDWR);
    try {
        std::vector<std::pair<uint32_t, Event>> handlers;
        {
            std::lock_guard guard(events_mutex);
            for (const auto& [key, handler] : events)
                if (handler)
                    handlers.emplace_back(key.first, handler);
        }
        for (const auto& [opcode, handler] : handlers)
            failed_event(opcode, handler);
    } catch (...) {}
}

std::shared_ptr<std::atomic<uint32_t>> Connection::flag(uint64_t id) {
    std::lock_guard guard(flags_mutex);
    auto& result = flags[id];
    if (!result)
        result = std::make_shared<std::atomic<uint32_t>>(0);
    return result;
}

void Connection::notifications() noexcept {
    try {
        while (alive.load()) {
            Buffer body;
            uint32_t opcode = 0;
            if (!read_any_frame(control, opcode, body) || body.bytes.size() < 8)
                break;
            const auto id = body.get(8);
            if (opcode != 100) {
                if (opcode < 101 || opcode > 103)
                    break;
                deliver(opcode, id, std::move(body));
                continue;
            }
            if (body.bytes.size() != 12)
                break;
            flag(id)->store(static_cast<uint32_t>(body.get(4)));
            std::lock_guard guard(flags_mutex);
            if (flags.size() > 1024) {
                for (auto it = flags.begin(); it != flags.end() && flags.size() > 1024;) {
                    if (it->second.use_count() == 1)
                        it = flags.erase(it);
                    else
                        ++it;
                }
            }
        }
    } catch (...) {
    }
    fail();
}

void Connection::failed_event(
    uint32_t opcode,
    const Event& handler
) {
    Buffer body;
    if (opcode == 101) {
        body.u32(COR_UNAVAILABLE);
    } else if (opcode == 103) {
        body.u32(0);
        body.u32(0);
        body.u32(1);
        body.u32(COR_UNAVAILABLE);
        body.u64(0);
    } else {
        return;
    }
    handler(std::move(body));
}

void Connection::watch(
    uint32_t opcode,
    uint64_t id,
    Event handler
) {
    {
        std::lock_guard guard(events_mutex);
        events[std::pair{ opcode, id }] = handler;
    }
    if (!alive.load())
        failed_event(opcode, handler);
}

void Connection::unwatch(
    uint32_t opcode,
    uint64_t id
) {
    std::lock_guard guard(events_mutex);
    events.erase(std::pair{ opcode, id });
}

void Connection::deliver(
    uint32_t opcode,
    uint64_t id,
    Buffer body
) {
    Event handler;
    {
        std::lock_guard guard(events_mutex);
        const auto key = std::pair{ opcode, id };
        auto it = events.find(key);
        // Watches are installed before the RPC that enables notifications.
        // Late messages after a successful release can be discarded.
        if (it == events.end())
            return;
        handler = it->second;
    }
    if (handler)
        handler(std::move(body));
}

cor_status_t Connection::rpc(
    uint32_t opcode,
    Buffer arguments,
    Buffer& response
) {
    if (!alive.load())
        return COR_UNAVAILABLE;
    Fd socket(connect_socket(endpoint));
    if (socket.value < 0) {
        fail();
        return COR_UNAVAILABLE;
    }
    Buffer body;
    body.bytes = token;
    body.bytes.insert(body.bytes.end(), arguments.bytes.begin(), arguments.bytes.end());
    if (!send_frame(socket.value, opcode, body.bytes) ||
        !read_frame(socket.value, opcode, response) || response.bytes.size() < 4) {
        fail();
        return COR_UNAVAILABLE;
    }
    return static_cast<cor_status_t>(response.get(4));
}

} // namespace corby::client
