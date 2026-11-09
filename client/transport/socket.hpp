/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <cstdint>
#include <string>
#include <unistd.h>

namespace corby::client {

struct Fd {
    int value = -1;
    explicit Fd(int v = -1) : value(v) {}
    Fd(const Fd&) = delete;
    ~Fd() {
        if (value >= 0)
            ::close(value);
    }
};

int connect_socket(const std::string& path);

} // namespace corby::client
