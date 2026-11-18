/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <utility>

#include "api/handles.hpp"
#include "api/support.hpp"

using namespace corby::client;

extern "C" {

cor_status_t cor_engine_register(
    const cor_context_t* context,
    const cor_engine_options_t* options,
    cor_engine_t** result
) {
    return protect([&]() -> cor_status_t {
        if (!result)
            return COR_INVALID_ARGUMENT;
        *result = nullptr;
        if (!context)
            return COR_INVALID_ARGUMENT;
        uint32_t size;
        auto status = prefix(options, offsetof(cor_engine_options_t, capabilities) + 8, size);
        if (status)
            return status;
        if (options->reserved || (options->capabilities & ~(COR_CAP_COOPERATIVE_YIELD | COR_CAP_MEMORY_RECLAIM)))
            return COR_UNSUPPORTED;
        if (!options->label || !options->device_ids || !options->device_count ||
            options->device_count > 4096)
            return COR_INVALID_ARGUMENT;
        auto c = context->client;
        uint64_t id = 0;
        if (c->api) {
            cor_engine_t* pointer = nullptr;
            status = c->api->engine_register(c->native, options, &pointer);
            id = reinterpret_cast<uintptr_t>(pointer);
        } else {
            Buffer args, response;
            args.string(options->label);
            args.u32(options->device_count);
            for (uint32_t i = 0; i < options->device_count; ++i)
                args.u32(static_cast<uint32_t>(options->device_ids[i]));
            args.u32(options->weight);
            args.u32(options->tenancy);
            args.u64(options->capabilities);
            status = c->rpc(8, std::move(args), response);
            if (!status)
                id = response.get(8);
        }
        if (status)
            return status;
        *result = new cor_engine{{c, id}};
        return COR_OK;
    });
}

cor_status_t cor_engine_retire(const cor_engine_t* handle) {
    return protect([&]() -> cor_status_t {
        if (!handle)
            return COR_INVALID_ARGUMENT;
        auto c = handle->client;
        if (c->api)
            return c->api->engine_retire(native<cor_engine_t>(handle->id));
        Buffer args, response;
        args.u64(handle->id);
        return c->rpc(9, std::move(args), response);
    });
}

cor_status_t cor_engine_unregister(cor_engine_t* handle) {
    return protect([&]() -> cor_status_t {
        if (!handle)
            return COR_OK;
        auto c = handle->client;
        if (const auto status = handle->callbacks->close(); status)
            return status;
        cor_status_t status;
        if (c->api)
            status = c->api->engine_unregister(native<cor_engine_t>(handle->id));
        else {
            Buffer args, response;
            args.u64(handle->id);
            status = c->rpc(10, std::move(args), response);
        }
        if (!status) {
            c->unwatch(102, handle->id);
            delete handle;
        } else {
            std::lock_guard guard(handle->callbacks->mutex);
            handle->callbacks->closed = false;
        }
        return status;
    });
}

cor_status_t cor_engine_set_weight(
    const cor_engine_t* engine,
    uint32_t weight
) {
    return protect([&]() -> cor_status_t {
        if (!engine || !weight)
            return COR_INVALID_ARGUMENT;
        auto c = engine->client;
        if (c->api)
            return c->api->engine_set_weight(native<cor_engine_t>(engine->id), weight);
        Buffer args, response;
        args.u64(engine->id);
        args.u32(weight);
        return c->rpc(11, std::move(args), response);
    });
}

cor_status_t cor_engine_stats(
    const cor_engine_t* handle,
    cor_engine_stats_t* result
) {
    return protect([&]() -> cor_status_t {
        if (!handle)
            return COR_INVALID_ARGUMENT;
        uint32_t size;
        auto status = prefix(result, 8, size);
        if (status)
            return status;
        auto c = handle->client;
        if (c->api)
            return c->api->engine_stats(native<cor_engine_t>(handle->id), result);
        Buffer args, response;
        args.u64(handle->id);
        status = c->rpc(12, std::move(args), response);
        return status ? status : copy_stats(response, result);
    });
}

cor_status_t cor_engine_label(
    const cor_engine_t* handle,
    char* out,
    uint32_t capacity,
    uint32_t* needed
) {
    return protect([&]() -> cor_status_t {
        if (!handle || !needed)
            return COR_INVALID_ARGUMENT;
        auto c = handle->client;
        if (c->api)
            return c->api->engine_label(native<cor_engine_t>(handle->id), out, capacity, needed);
        Buffer args, response;
        args.u64(handle->id);
        const auto status = c->rpc(13, std::move(args), response);
        return status ? status : copy_label(response, out, capacity, needed);
    });
}

} // extern "C"
