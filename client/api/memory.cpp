/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <algorithm>
#include <cstring>
#include <utility>

#include "api/extensions.hpp"
#include "api/support.hpp"

using namespace corby::client;

extern "C" {

cor_status_t cor_engine_set_memory_handler(
    const cor_engine_t* engine,
    cor_memory_handler_fn handler,
    void* context
) {
    return protect([&]() -> cor_status_t {
        if (!engine)
            return COR_INVALID_ARGUMENT;
        auto c = engine->client;
        if (!COR_HAS(c, engine_set_memory_handler, COR_FEATURE_MEMORY_LEASES))
            return COR_UNSUPPORTED;
        auto callbacks = engine->callbacks;
        {
            std::lock_guard guard(callbacks->mutex);
            if (callbacks->active)
                return COR_BUSY;
            callbacks->memory = handler;
            callbacks->memory_context = context;
        }
        if (c->api) {
            return c->api->engine_set_memory_handler(
                native<cor_engine_t>(engine->id),
                handler ? +[](cor_reclaim_t* raw, void* owner) {
                    auto* engine = static_cast<const cor_engine_t*>(owner);
                    auto callbacks = engine->callbacks;
                    callbacks->request(new cor_reclaim{{engine->client, reinterpret_cast<uintptr_t>(raw)}});
                } : nullptr,
                const_cast<cor_engine_t*>(engine)
            );
        }
        std::weak_ptr<Callbacks> weak = callbacks;
        std::weak_ptr<Connection> connection = c;
        c->watch(102, engine->id, [weak, connection](Buffer body) {
            auto c = connection.lock();
            if (!c)
                return;
            auto* request = new cor_reclaim{{c, body.get(8)}};
            if (auto callbacks = weak.lock())
                callbacks->request(request);
            else
                (void)cor_reclaim_complete(request, COR_CANCELLED, nullptr);
        });
        Buffer args, response;
        args.u64(engine->id);
        args.u32(handler != nullptr);
        return c->rpc(23, std::move(args), response);
    });
}

cor_status_t cor_lease_reclaim_memory(
    cor_lease_t* lease,
    const cor_memory_options_t* options,
    cor_memory_progress_fn handler,
    void* context
) {
    return protect([&]() -> cor_status_t {
        if (!lease || !handler)
            return COR_INVALID_ARGUMENT;
        uint32_t size;
        if (auto s = prefix(options, offsetof(cor_memory_options_t, timeout_ns) + 8, size); s)
            return s;
        if (!options->requirements || !options->requirement_count ||
            options->requirement_count > 4096 || options->reserved)
            return COR_INVALID_ARGUMENT;
        for (uint32_t i = 0; i < options->requirement_count; ++i)
            if (options->requirements[i].reserved)
                return COR_INVALID_ARGUMENT;
        auto c = lease->client;
        if (!COR_HAS(c, lease_reclaim_memory, COR_FEATURE_MEMORY_LEASES))
            return COR_UNSUPPORTED;
        auto callbacks = lease->callbacks;
        {
            std::lock_guard guard(callbacks->mutex);
            if (callbacks->progress || callbacks->closed)
                return COR_BUSY;
            callbacks->lease = lease;
            callbacks->progress = handler;
            callbacks->progress_context = context;
        }
        cor_status_t status;
        if (c->api) {
            status = c->api->lease_reclaim_memory(
                native<cor_lease_t>(lease->id), options,
                [](cor_lease_t*, const cor_memory_progress_t* progress, void* raw) {
                    auto callbacks = static_cast<cor_lease_t*>(raw)->callbacks;
                    cor_memory_progress_t snapshot{};
                    std::memcpy(&snapshot, progress, std::min<size_t>(progress->size, sizeof(snapshot)));
                    callbacks->report(snapshot);
                }, lease
            );
        } else {
            std::weak_ptr<Callbacks> weak = callbacks;
            c->watch(103, lease->id, [weak](Buffer body) {
                const cor_memory_progress_t progress{
                    .size = sizeof(cor_memory_progress_t),
                    .abi_version = COR_ABI_VERSION,
                    .pending = static_cast<uint32_t>(body.get(4)),
                    .completed = static_cast<uint32_t>(body.get(4)),
                    .failed = static_cast<uint32_t>(body.get(4)),
                    .status = static_cast<cor_status_t>(body.get(4)),
                    .released_bytes = body.get(8)
                };
                if (auto callbacks = weak.lock())
                    callbacks->report(progress);
            });
            Buffer args, response;
            args.u64(lease->id);
            args.u64(options->timeout_ns);
            args.u32(options->requirement_count);
            for (uint32_t i = 0; i < options->requirement_count; ++i) {
                if (options->requirements[i].reserved)
                    return COR_INVALID_ARGUMENT;
                args.u32(static_cast<uint32_t>(options->requirements[i].device_id));
                args.u64(options->requirements[i].bytes);
            }
            status = c->rpc(24, std::move(args), response);
        }
        if (status) {
            std::lock_guard guard(callbacks->mutex);
            callbacks->progress = nullptr;
        }
        return status;
    });
}

cor_status_t cor_reclaim_requirements(
    const cor_reclaim_t* request,
    cor_memory_requirement_t* requirements,
    uint32_t capacity,
    uint32_t* needed
) {
    return protect([&]() -> cor_status_t {
        if (!request || !needed || (!requirements && capacity))
            return COR_INVALID_ARGUMENT;
        auto c = request->client;
        if (c->api)
            return c->api->reclaim_requirements(native<cor_reclaim_t>(request->id), requirements, capacity, needed);
        Buffer args, response;
        args.u64(request->id);
        if (auto s = c->rpc(25, std::move(args), response); s)
            return s;
        *needed = static_cast<uint32_t>(response.get(4));
        if (!requirements)
            return COR_OK;
        if (capacity < *needed)
            return COR_BUSY;
        for (uint32_t i = 0; i < *needed; ++i) {
            requirements[i] = {
                .device_id = static_cast<int32_t>(response.get(4)),
                .reserved = 0,
                .bytes = response.get(8)
            };
        }
        return COR_OK;
    });
}

cor_status_t cor_reclaim_complete(
    cor_reclaim_t* request,
    cor_status_t outcome,
    const cor_memory_release_report_t* report
) {
    return protect([&]() -> cor_status_t {
        if (!request || outcome < COR_OK || outcome >= COR_PENDING)
            return COR_INVALID_ARGUMENT;
        uint32_t size;
        if (report) {
            if (auto s = prefix(report, 16, size); s)
                return s;
        }
        auto c = request->client;
        cor_status_t status;
        if (c->api)
            status = c->api->reclaim_complete(native<cor_reclaim_t>(request->id), outcome, report);
        else {
            Buffer args, response;
            args.u64(request->id);
            args.u32(static_cast<uint32_t>(outcome));
            args.u64(report ? report->released_bytes : 0);
            status = c->rpc(26, std::move(args), response);
        }
        if (!status)
            delete request;
        return status;
    });
}

} // extern "C"
