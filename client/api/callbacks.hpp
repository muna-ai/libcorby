/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "corby/corby.h"

namespace corby::client {

// Shared by the public handle and in-flight delivery. Closing suppresses queued
// callbacks; active callbacks make close BUSY so borrowed contexts stay alive.
struct Callbacks {
    std::mutex mutex;
    bool closed = false;
    uint32_t active = 0;
    cor_status_t status = COR_PENDING;
    bool registered = false;
    bool notified = false;
    bool progress_terminal = false;
    uint32_t progress_completed = 0;
    bool progress_running = false;
    std::optional<cor_memory_progress_t> next_progress;
    cor_lease_t* lease = nullptr;
    cor_lease_acquisition_fn acquisition = nullptr;
    void* acquisition_context = nullptr;
    cor_memory_handler_fn memory = nullptr;
    void* memory_context = nullptr;
    cor_memory_progress_fn progress = nullptr;
    void* progress_context = nullptr;

    cor_status_t close();
    void complete(cor_status_t outcome);
    void report(const cor_memory_progress_t& value);
    void request(cor_reclaim_t* value);
};

} // namespace corby::client
