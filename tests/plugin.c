/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include "corby/corby.h"

/* Two independently dlopened clients each open their own context. */
cor_status_t plugin_register(cor_engine_t** result) {
    cor_context_t* context = 0;
    cor_status_t status = cor_context_create(0, &context);
    if (status != COR_OK)
        return status;
    const int32_t device = 0;
    const cor_engine_options_t options = {
        .size = sizeof(options),
        .abi_version = COR_ABI_VERSION,
        .label = "plugin",
        .device_ids = &device,
        .device_count = 1,
        .weight = 1,
        .capabilities = COR_CAP_COOPERATIVE_YIELD
    };
    status = cor_engine_register(context, &options, result);
    cor_context_destroy(context);
    return status;
}

cor_status_t plugin_begin(cor_engine_t* engine, cor_work_t** work) {
    return cor_work_begin(engine, work);
}

cor_status_t plugin_acquire(cor_work_t* work, cor_lease_t** lease) {
    return cor_lease_acquire(work, 0, lease);
}

cor_status_t plugin_release(cor_lease_t* lease) {
    return cor_lease_release(lease);
}

cor_status_t plugin_end(cor_work_t* work) {
    return cor_work_end(work);
}

cor_status_t plugin_unregister(cor_engine_t* engine) {
    return cor_engine_unregister(engine);
}
