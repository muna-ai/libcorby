/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <utility>

#include "api/handles.hpp"
#include "api/support.hpp"

using namespace corby::client;

extern "C" {

cor_status_t cor_work_begin(
    const cor_engine_t* engine,
    cor_work_t** result
) {
    return protect([&]() -> cor_status_t {
        if (!result)
            return COR_INVALID_ARGUMENT;
        *result = nullptr;
        if (!engine)
            return COR_INVALID_ARGUMENT;
        auto c = engine->client;
        uint64_t id = 0;
        cor_status_t status;
        if (c->api) {
            cor_work_t* pointer = nullptr;
            status = c->api->work_begin(native<cor_engine_t>(engine->id), &pointer);
            id = reinterpret_cast<uintptr_t>(pointer);
        } else {
            Buffer args, response;
            args.u64(engine->id);
            status = c->rpc(14, std::move(args), response);
            if (!status)
                id = response.get(8);
        }
        if (status)
            return status;
        *result = new cor_work{{c, id}};
        return COR_OK;
    });
}

cor_status_t cor_work_cancel(const cor_work_t* handle) {
    return protect([&]() -> cor_status_t {
        if (!handle)
            return COR_INVALID_ARGUMENT;
        auto c = handle->client;
        if (c->api)
            return c->api->work_cancel(native<cor_work_t>(handle->id));
        Buffer args, response;
        args.u64(handle->id);
        return c->rpc(15, std::move(args), response);
    });
}

cor_status_t cor_work_end(cor_work_t* handle) {
    return protect([&]() -> cor_status_t {
        if (!handle)
            return COR_OK;
        auto c = handle->client;
        cor_status_t status;
        if (c->api)
            status = c->api->work_end(native<cor_work_t>(handle->id));
        else {
            Buffer args, response;
            args.u64(handle->id);
            status = c->rpc(16, std::move(args), response);
        }
        if (!status)
            delete handle;
        return status;
    });
}

} // extern "C"
