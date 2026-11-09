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
    alive.store(false);
    if (control >= 0)
        ::shutdown(control, SHUT_RDWR);
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
            if (!read_frame(control, 100, body) || body.bytes.size() != 12)
                break;
            const auto id = body.get(8);
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
    alive.store(false);
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
