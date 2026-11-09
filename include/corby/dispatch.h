/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once
#include <stddef.h>
#include "corby/corby.h"

/* 
 * Immutable append-only dispatch table. Addresses never cross processes.
 */
typedef struct cor_dispatch_v1 {
    uint32_t size;
    uint32_t abi_version;
    uint64_t features;
    cor_status_t (*context_create)(
        const cor_context_options_t* options,
        cor_context_t** result
    );
    cor_status_t (*context_destroy)(cor_context_t* context);
    cor_status_t (*context_engine_ids)(
        const cor_context_t* context,
        uint64_t* ids,
        uint32_t capacity,
        uint32_t* needed
    );
    cor_status_t (*context_engine_stats)(
        const cor_context_t* context,
        uint64_t id,
        cor_engine_stats_t* result
    );
    cor_status_t (*context_engine_devices)(
        const cor_context_t* context,
        uint64_t id,
        int32_t* devices,
        uint32_t capacity,
        uint32_t* needed
    );
    cor_status_t (*context_engine_set_weight)(
        const cor_context_t* context,
        uint64_t id,
        uint32_t weight
    );
    cor_status_t (*context_engine_label)(
        const cor_context_t* context,
        uint64_t id,
        char* buffer,
        uint32_t capacity,
        uint32_t* needed
    );
    cor_status_t (*engine_register)(
        const cor_context_t* context,
        const cor_engine_options_t* options,
        cor_engine_t** result
    );
    cor_status_t (*engine_retire)(const cor_engine_t* engine);
    cor_status_t (*engine_unregister)(cor_engine_t* engine);
    cor_status_t (*engine_set_weight)(
        const cor_engine_t* engine,
        uint32_t weight
    );
    cor_status_t (*engine_stats)(
        const cor_engine_t* engine,
        cor_engine_stats_t* result
    );
    cor_status_t (*engine_label)(
        const cor_engine_t* engine,
        char* buffer,
        uint32_t capacity,
        uint32_t* needed
    );
    cor_status_t (*work_begin)(
        const cor_engine_t* engine,
        cor_work_t** result
    );
    cor_status_t (*work_cancel)(const cor_work_t* work);
    cor_status_t (*work_end)(cor_work_t* work);
    cor_status_t (*lease_acquire)(
        const cor_work_t* work,
        uint64_t timeout_ns,
        cor_lease_t** result
    );
    cor_status_t (*lease_should_yield)(
        const cor_lease_t* lease,
        uint64_t min_hold_ns,
        uint32_t* result
    );
    cor_status_t (*lease_release)(cor_lease_t* lease);
} cor_dispatch_v1_t;

/* Freeze this original prefix even when optional entries are appended. */
#define COR_DISPATCH_V1_REQUIRED_SIZE (offsetof(cor_dispatch_v1_t, lease_release) + sizeof(((cor_dispatch_v1_t*)0)->lease_release))
