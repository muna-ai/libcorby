/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include "api/callbacks.hpp"

namespace corby::client {

cor_status_t Callbacks::close() {
    std::lock_guard guard(mutex);
    if (active)
        return COR_BUSY;
    closed = true;
    return COR_OK;
}

void Callbacks::complete(cor_status_t outcome) {
    cor_lease_acquisition_fn handler;
    void* context;
    {
        std::lock_guard guard(mutex);
        if (status == COR_PENDING)
            status = outcome;
        if (closed || !registered || notified)
            return;
        notified = true;
        ++active;
        handler = acquisition;
        context = acquisition_context;
    }
    try { handler(lease, context); } catch (...) {}
    std::lock_guard guard(mutex);
    --active;
}

void Callbacks::report(const cor_memory_progress_t& value) {
    std::unique_lock lock(mutex);
    if (closed || !progress || progress_terminal || value.completed < progress_completed)
        return;
    progress_completed = value.completed;
    progress_terminal = value.status != COR_PENDING;
    next_progress = value;
    if (progress_running)
        return;
    progress_running = true;
    ++active;
    while (next_progress) {
        const auto snapshot = *next_progress;
        next_progress.reset();
        const auto handler = progress;
        const auto context = progress_context;
        lock.unlock();
        try { handler(lease, &snapshot, context); } catch (...) {}
        lock.lock();
    }
    progress_running = false;
    --active;
}

void Callbacks::request(cor_reclaim_t* value) {
    cor_memory_handler_fn handler;
    void* context;
    {
        std::lock_guard guard(mutex);
        handler = closed ? nullptr : memory;
        context = memory_context;
        if (handler)
            ++active;
    }
    if (!handler) {
        (void)cor_reclaim_complete(value, COR_UNSUPPORTED, nullptr);
        return;
    }
    // Ownership of value transfers to the handler, which may queue it for the
    // engine's safe boundary. Exceptions must not escape across the C ABI.
    try { handler(value, context); } catch (...) {}
    std::lock_guard guard(mutex);
    --active;
}

} // namespace corby::client
