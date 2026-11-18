/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include "corby/corby.h"
#include "corby/dispatch.h"
#include "corby/runtime.h"

#define CHECK(expr) do { if (!(expr)) return __LINE__; } while (0)

/* Freeze original field offsets while allowing new fields after the prefix. */
_Static_assert(offsetof(cor_engine_options_t, label) == 8, "label ABI");
_Static_assert(offsetof(cor_engine_options_t, capabilities) == 24 + 2 * sizeof(void*), "capability ABI");
_Static_assert(offsetof(cor_engine_stats_t, active_work) == 8, "stats ABI");
_Static_assert(offsetof(cor_engine_stats_t, capabilities) == 56, "stats capability ABI");

_Static_assert(offsetof(cor_dispatch_v1_t, work_begin_ex) == COR_DISPATCH_V1_REQUIRED_SIZE, "original dispatch prefix");
_Static_assert(offsetof(cor_acquire_options_t, timeout_ns) == 16, "acquire options ABI");
_Static_assert(offsetof(cor_memory_progress_t, released_bytes) == 24, "progress ABI");

int main(void) {
    cor_runtime_t* runtime = NULL;
    CHECK(cor_runtime_start(NULL, &runtime) == COR_OK);

    CHECK(sizeof(cor_status_t) == 4);
    CHECK(cor_abi_version() == COR_ABI_VERSION);
    CHECK((cor_features() & COR_FEATURE_GROUPS) != 0);
    cor_context_t* context = NULL;
    const uint32_t old_prefix[] = { 8, COR_ABI_VERSION };
    CHECK(cor_context_create((const cor_context_options_t*)old_prefix, &context) == COR_OK);
    const int32_t devices[] = { 0, 1 };
    struct extended_options {
        cor_engine_options_t known;
        uint64_t future;
    } options = {
        .known = {
            .size = sizeof(options), .abi_version = COR_ABI_VERSION,
            .label = "c-client", .device_ids = devices, .device_count = 2,
            .weight = 1, .tenancy = COR_TENANCY_SHARED,
            .capabilities = COR_CAP_COOPERATIVE_YIELD
        },
        .future = UINT64_MAX
    };
    cor_engine_t* engine = NULL;
    CHECK(cor_engine_register(context, &options.known, &engine) == COR_OK);
    cor_context_t* supervisor = NULL;
    CHECK(cor_context_create(NULL, &supervisor) == COR_OK);
    uint32_t count = 0;
    CHECK(cor_context_engine_ids(supervisor, NULL, 0, &count) == COR_OK && count == 1);
    uint64_t id = 0;
    CHECK(cor_context_engine_ids(supervisor, &id, 1, &count) == COR_OK && id != 0);
    CHECK(cor_context_engine_set_weight(supervisor, id, 3) == COR_OK);
    int32_t assigned[2] = { -1, -1 };
    CHECK(cor_context_engine_devices(supervisor, id, assigned, 2, &count) == COR_OK);
    CHECK(count == 2 && assigned[0] == 0 && assigned[1] == 1);
    char supervised_label[16];
    CHECK(cor_context_engine_label(supervisor, id, supervised_label, 16, &count) == COR_OK);
    CHECK(strcmp(supervised_label, "c-client") == 0);
    cor_engine_stats_t observed = { .size = sizeof(observed), .abi_version = COR_ABI_VERSION };
    CHECK(cor_context_engine_stats(supervisor, id, &observed) == COR_OK && observed.weight == 3);
    CHECK(cor_context_destroy(context) == COR_OK); /* Children retain state. */
    cor_work_t* work = NULL;
    CHECK(cor_work_begin(engine, &work) == COR_OK);
    CHECK(cor_engine_unregister(engine) == COR_BUSY);
    cor_lease_t* lease = NULL;
    CHECK(cor_lease_acquire(work, 0, &lease) == COR_OK);
    CHECK(cor_work_end(work) == COR_BUSY);
    cor_lease_t* duplicate = NULL;
    CHECK(cor_lease_acquire(work, 0, &duplicate) == COR_BUSY);
    CHECK(duplicate == NULL);

    /* Simulate an older stats consumer with a guard against over-writing. */
    struct old_stats { uint32_t size, abi; uint64_t work, sentinel; } old = {
        .size = offsetof(struct old_stats, sentinel), .abi = COR_ABI_VERSION,
        .sentinel = UINT64_C(0x123456789abcdef0)
    };
    CHECK(cor_engine_stats(engine, (cor_engine_stats_t*)&old) == COR_OK);
    CHECK(old.work == 1 && old.sentinel == UINT64_C(0x123456789abcdef0));
    CHECK(old.size == sizeof(cor_engine_stats_t));
    struct new_stats { cor_engine_stats_t known; uint64_t future; } newer = {
        .known = { .size = sizeof(newer), .abi_version = COR_ABI_VERSION },
        .future = UINT64_MAX
    };
    CHECK(cor_engine_stats(engine, &newer.known) == COR_OK);
    CHECK(newer.known.holding == 1 && newer.future == UINT64_MAX);
    uint32_t needed = 0;
    CHECK(cor_engine_label(engine, NULL, 0, &needed) == COR_OK && needed == 9);
    char label[16];
    CHECK(cor_engine_label(engine, label, sizeof(label), &needed) == COR_OK);
    CHECK(strcmp(label, "c-client") == 0);
    CHECK(cor_work_cancel(work) == COR_OK);
    uint32_t yield = 0;
    CHECK(cor_lease_should_yield(lease, UINT64_MAX, &yield) == COR_OK && yield == 1);
    CHECK(cor_lease_release(lease) == COR_OK);
    CHECK(cor_work_end(work) == COR_OK);
    CHECK(cor_engine_retire(engine) == COR_OK);
    CHECK(cor_work_begin(engine, &work) == COR_RETIRED && work == NULL);
    CHECK(cor_engine_unregister(engine) == COR_OK);
    CHECK(cor_context_engine_stats(supervisor, id, &observed) == COR_NOT_FOUND);
    CHECK(cor_context_engine_set_weight(supervisor, id, 1) == COR_NOT_FOUND);
    CHECK(cor_context_destroy(supervisor) == COR_OK);
    CHECK(cor_lease_release(NULL) == COR_OK);
    CHECK(cor_runtime_destroy(runtime) == COR_OK);
    return 0;
}
