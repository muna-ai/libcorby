/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <utility>

#include "api/handles.hpp"
#include "api/support.hpp"

using namespace corby::client;

extern "C" {

cor_status_t cor_lease_acquire(
    const cor_work_t* work,
    uint64_t timeout_ns,
    cor_lease_t** result
) {
    return protect([&]() -> cor_status_t {
        if (!result)
            return COR_INVALID_ARGUMENT;
        *result = nullptr;
        if (!work)
            return COR_INVALID_ARGUMENT;
        auto c = work->client;
        uint64_t id = 0;
        cor_status_t status;
        if (c->api) {
            cor_lease_t* pointer = nullptr;
            status = c->api->lease_acquire(native<cor_work_t>(work->id), timeout_ns, &pointer);
            id = reinterpret_cast<uintptr_t>(pointer);
        } else {
            Buffer args, response;
            args.u64(work->id);
            args.u64(timeout_ns);
            status = c->rpc(17, std::move(args), response);
            if (!status)
                id = response.get(8);
        }
        if (status)
            return status;
        *result = new cor_lease{{c, id}, c->api ? nullptr : c->flag(id), Clock::now()};
        (*result)->resource = work->resource;
        (*result)->callbacks->status = COR_OK;
        return COR_OK;
    });
}

cor_status_t cor_lease_should_yield(
    const cor_lease_t* lease,
    uint64_t min_hold_ns,
    uint32_t* result
) {
    return protect([&]() -> cor_status_t {
        if (!result)
            return COR_INVALID_ARGUMENT;
        *result = 1;
        if (!lease)
            return COR_INVALID_ARGUMENT;
        auto c = lease->client;
        if (c->api)
            return c->api->lease_should_yield(native<cor_lease_t>(lease->id), min_hold_ns, result);
        if (!c->alive.load())
            return COR_UNAVAILABLE;
        if (lease->resource != COR_RESOURCE_COMPUTE)
            return COR_UNSUPPORTED;
        {
            std::lock_guard guard(lease->callbacks->mutex);
            if (lease->callbacks->status != COR_OK)
                return lease->callbacks->status;
        }
        const auto flags = lease->flags->load();
        const auto held =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - lease->started)
                .count();
        *result = (flags & 2) || ((flags & 1) && static_cast<uint64_t>(held) >= min_hold_ns);
        return COR_OK;
    });
}

cor_status_t cor_lease_release(cor_lease_t* handle) {
    return protect([&]() -> cor_status_t {
        if (!handle)
            return COR_OK;
        auto c = handle->client;
        if (const auto status = handle->callbacks->close(); status)
            return status;
        cor_status_t status;
        if (c->api)
            status = c->api->lease_release(native<cor_lease_t>(handle->id));
        else {
            Buffer args, response;
            args.u64(handle->id);
            status = c->rpc(19, std::move(args), response);
        }
        if (!status) {
            c->unwatch(101, handle->id);
            c->unwatch(103, handle->id);
            delete handle;
        } else {
            std::lock_guard guard(handle->callbacks->mutex);
            handle->callbacks->closed = false;
        }
        return status;
    });
}

} // extern "C"
