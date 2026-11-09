/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <utility>

#include "api/handles.hpp"
#include "api/support.hpp"

using namespace corby::client;

extern "C" {

cor_status_t cor_context_engine_set_weight(
    const cor_context_t* context,
    uint64_t id,
    uint32_t weight
) {
    return protect([&]() -> cor_status_t {
        if (!context || !weight)
            return COR_INVALID_ARGUMENT;
        auto c = context->client;
        if (c->api)
            return c->api->context_engine_set_weight(c->native, id, weight);
        Buffer args, response;
        args.u64(id);
        args.u32(weight);
        return c->rpc(6, std::move(args), response);
    });
}

cor_status_t cor_context_engine_stats(
    const cor_context_t* handle,
    uint64_t id,
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
            return c->api->context_engine_stats(c->native, id, result);
        Buffer args, response;
        args.u64(id);
        status = c->rpc(4, std::move(args), response);
        return status ? status : copy_stats(response, result);
    });
}

cor_status_t cor_context_engine_label(
    const cor_context_t* handle,
    uint64_t id,
    char* out,
    uint32_t capacity,
    uint32_t* needed
) {
    return protect([&]() -> cor_status_t {
        if (!handle || !needed)
            return COR_INVALID_ARGUMENT;
        auto c = handle->client;
        if (c->api)
            return c->api->context_engine_label(c->native, id, out, capacity, needed);
        Buffer args, response;
        args.u64(id);
        const auto status = c->rpc(7, std::move(args), response);
        return status ? status : copy_label(response, out, capacity, needed);
    });
}

cor_status_t cor_context_engine_ids(
    const cor_context_t* context,
    uint64_t* out,
    uint32_t capacity,
    uint32_t* needed
) {
    return protect([&]() -> cor_status_t {
        if (!context || !needed)
            return COR_INVALID_ARGUMENT;
        auto c = context->client;
        if (c->api)
            return c->api->context_engine_ids(c->native, out, capacity, needed);
        Buffer args, response;
        const auto status = c->rpc(3, std::move(args), response);
        if (status)
            return status;
        *needed = static_cast<uint32_t>(response.get(4));
        if (!out)
            return capacity == 0 ? COR_OK : COR_INVALID_ARGUMENT;
        if (capacity < *needed)
            return COR_BUSY;
        for (uint32_t i = 0; i < *needed; ++i)
            out[i] = static_cast<uint64_t>(response.get(sizeof(uint64_t)));
        return COR_OK;
    });
}

cor_status_t cor_context_engine_devices(
    const cor_context_t* context,
    uint64_t id,
    int32_t* out,
    uint32_t capacity,
    uint32_t* needed
) {
    return protect([&]() -> cor_status_t {
        if (!context || !needed)
            return COR_INVALID_ARGUMENT;
        auto c = context->client;
        if (c->api)
            return c->api->context_engine_devices(c->native, id, out, capacity, needed);
        Buffer args, response;
        args.u64(id);
        const auto status = c->rpc(5, std::move(args), response);
        if (status)
            return status;
        *needed = static_cast<uint32_t>(response.get(4));
        if (!out)
            return capacity == 0 ? COR_OK : COR_INVALID_ARGUMENT;
        if (capacity < *needed)
            return COR_INVALID_ARGUMENT;
        for (uint32_t i = 0; i < *needed; ++i)
            out[i] = static_cast<int32_t>(response.get(sizeof(int32_t)));
        return COR_OK;
    });
}

} // extern "C"
