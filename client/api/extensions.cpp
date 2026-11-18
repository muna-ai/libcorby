/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <utility>

#include "api/extensions.hpp"
#include "api/support.hpp"

using namespace corby::client;

extern "C" {

cor_status_t cor_work_begin_ex(
    const cor_engine_t* engine,
    const cor_work_options_t* options,
    cor_work_t** result
) {
    return protect([&]() -> cor_status_t {
        if (!engine || !result)
            return COR_INVALID_ARGUMENT;
        *result = nullptr;
        if (!options)
            return cor_work_begin(engine, result);
        uint32_t size;
        if (auto s = prefix(options, 16, size); s)
            return s;
        auto c = engine->client;
        if (!COR_HAS(c, work_begin_ex, COR_FEATURE_MEMORY_LEASES))
            return COR_UNSUPPORTED;
        uint64_t id = 0;
        cor_status_t status;
        if (c->api) {
            cor_work_t* raw = nullptr;
            status = c->api->work_begin_ex(native<cor_engine_t>(engine->id), options, &raw);
            id = reinterpret_cast<uintptr_t>(raw);
        } else {
            Buffer args, response;
            args.u64(engine->id);
            args.u32(options->resource);
            args.u32(options->intent);
            status = c->rpc(20, std::move(args), response);
            if (!status)
                id = response.get(8);
        }
        if (status)
            return status;
        *result = new cor_work{{c, id}, options->resource};
        return COR_OK;
    });
}

cor_status_t cor_lease_acquire_ex(
    const cor_work_t* work,
    const cor_acquire_options_t* options,
    cor_lease_t** result
) {
    return protect([&]() -> cor_status_t {
        if (!work || !result)
            return COR_INVALID_ARGUMENT;
        *result = nullptr;
        if (!options)
            return cor_lease_acquire(work, COR_WAIT_FOREVER, result);
        uint32_t size;
        if (auto s = prefix(options, 24, size); s)
            return s;
        if (options->reserved || (options->flags & ~COR_LEASE_ACQUIRE_FLAGS_ASYNC))
            return COR_UNSUPPORTED;
        auto c = work->client;
        if (!COR_HAS(c, lease_acquire_ex, COR_FEATURE_ASYNC_LEASES))
            return COR_UNSUPPORTED;
        uint64_t id = 0;
        cor_status_t status;
        if (c->api) {
            cor_lease_t* raw = nullptr;
            status = c->api->lease_acquire_ex(native<cor_work_t>(work->id), options, &raw);
            id = reinterpret_cast<uintptr_t>(raw);
        } else {
            Buffer args, response;
            args.u64(work->id);
            args.u32(options->flags);
            args.u64(options->timeout_ns);
            status = c->rpc(21, std::move(args), response);
            if (!status)
                id = response.get(8);
        }
        if (status)
            return status;
        auto* lease = new cor_lease{{c, id}, c->api ? nullptr : c->flag(id), Clock::now()};
        lease->resource = work->resource;
        lease->callbacks->lease = lease;
        *result = lease;
        return COR_OK;
    });
}

cor_status_t cor_lease_acquired(const cor_lease_t* lease) {
    return protect([&]() -> cor_status_t {
        if (!lease)
            return COR_INVALID_ARGUMENT;
        auto c = lease->client;
        if (!COR_HAS(c, lease_acquired, COR_FEATURE_ASYNC_LEASES))
            return COR_UNSUPPORTED;
        if (c->api)
            return c->api->lease_acquired(native<cor_lease_t>(lease->id));
        Buffer args, response;
        args.u64(lease->id);
        const auto status = c->rpc(22, std::move(args), response);
        if (status != COR_PENDING)
            lease->callbacks->complete(status);
        return status;
    });
}

cor_status_t cor_lease_register_handler(
    cor_lease_t* lease,
    cor_lease_acquisition_fn handler,
    void* context
) {
    return protect([&]() -> cor_status_t {
        if (!lease || !handler)
            return COR_INVALID_ARGUMENT;
        auto c = lease->client;
        if (!COR_HAS(c, lease_register_handler, COR_FEATURE_ASYNC_LEASES))
            return COR_UNSUPPORTED;
        auto callbacks = lease->callbacks;
        {
            std::lock_guard guard(callbacks->mutex);
            if (callbacks->registered || callbacks->closed)
                return COR_BUSY;
            callbacks->lease = lease;
            callbacks->registered = true;
            callbacks->acquisition = handler;
            callbacks->acquisition_context = context;
        }
        if (c->api) {
            const auto status = c->api->lease_register_handler(
                native<cor_lease_t>(lease->id),
                [](cor_lease_t*, void* raw) {
                    auto* lease = static_cast<cor_lease_t*>(raw);
                    auto callbacks = lease->callbacks;
                    callbacks->complete(cor_lease_acquired(lease));
                },
                lease
            );
            if (status) {
                std::lock_guard guard(callbacks->mutex);
                callbacks->registered = false;
            }
            return status;
        }
        std::weak_ptr<Callbacks> weak = callbacks;
        c->watch(101, lease->id, [weak](Buffer body) {
            if (auto cb = weak.lock())
                cb->complete(static_cast<cor_status_t>(body.get(4)));
        });
        Buffer args, response;
        args.u64(lease->id);
        const auto status = c->rpc(27, std::move(args), response);
        if (status) {
            c->unwatch(101, lease->id);
            std::lock_guard guard(callbacks->mutex);
            callbacks->registered = false;
        }
        return status;
    });
}

} // extern "C"
