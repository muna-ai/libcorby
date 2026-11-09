/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include "corby/corby.h"
#ifdef __cplusplus
extern "C" {
#endif

#define COR_SCOPE_PROCESS UINT32_C(0)
#define COR_SCOPE_SYSTEM UINT32_C(1)

typedef struct cor_runtime cor_runtime_t;
typedef struct cor_runtime_options {
    uint32_t size;
    uint32_t abi_version;
    uint32_t scope;
    uint32_t reserved;
    const char* endpoint;
} cor_runtime_options_t;

/* 
 * Host-only API; not part of Corby::client. NULL options selects Process.
 * System accepts same-process direct clients AND Unix socket clients.
 * An endpoint with Process scope is INVALID_ARGUMENT.
 */
cor_status_t cor_runtime_start(
    const cor_runtime_options_t* options,
    cor_runtime_t** result
);

/* 
 * Stops admission. BUSY preserves runtime and publication until children drain.
 * Successful shutdown invalidates attachment, not existing context handles.
 */
cor_status_t cor_runtime_shutdown(cor_runtime_t* runtime);

/*
 * BUSY does not consume the handle. NULL teardown is allowed.
 */
cor_status_t cor_runtime_destroy(cor_runtime_t* runtime);

#ifdef __cplusplus
}
#endif
