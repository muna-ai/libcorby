/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * ABI major stays fixed for compatible additions. These are integer constants,
 * not C enum types: their width and representation are part of the contract.
 */
#define COR_ABI_VERSION UINT32_C(1)
#define COR_WAIT_FOREVER UINT64_MAX
#define COR_CAP_COOPERATIVE_YIELD UINT64_C(1)
#define COR_CAP_MEMORY_RECLAIM UINT64_C(2)
#define COR_FEATURE_GROUPS UINT64_C(1)
#define COR_FEATURE_DEDICATED UINT64_C(2)
#define COR_FEATURE_CANCELLATION UINT64_C(4)
#define COR_FEATURE_WEIGHTED_FAIRNESS UINT64_C(8)
#define COR_FEATURE_MEMORY_LEASES UINT64_C(16)
#define COR_FEATURE_ASYNC_LEASES UINT64_C(32)
#define COR_RESOURCE_COMPUTE UINT32_C(0)
#define COR_RESOURCE_MEMORY UINT32_C(1)
#define COR_WORK_INTENT_REQUIRED UINT32_C(0)
#define COR_WORK_INTENT_INITIALIZATION UINT32_C(1)
#define COR_WORK_INTENT_OPPORTUNISTIC UINT32_C(2)
#define COR_LEASE_ACQUIRE_FLAGS_ASYNC UINT32_C(1)
#define COR_TENANCY_SHARED UINT32_C(0)
#define COR_TENANCY_DEDICATED UINT32_C(1)

typedef int32_t cor_status_t;
#define COR_OK INT32_C(0)
#define COR_INVALID_ARGUMENT INT32_C(1)
#define COR_UNSUPPORTED INT32_C(2)
#define COR_BUSY INT32_C(3)
#define COR_TIMEOUT INT32_C(4)
#define COR_CANCELLED INT32_C(5)
#define COR_CONFLICT INT32_C(6)
#define COR_RETIRED INT32_C(7)
#define COR_INTERNAL INT32_C(8)
#define COR_NOT_FOUND INT32_C(9)
#define COR_UNAVAILABLE INT32_C(10)
#define COR_PENDING INT32_C(11)

typedef struct cor_context cor_context_t;
typedef struct cor_engine cor_engine_t;
typedef struct cor_work cor_work_t;
typedef struct cor_lease cor_lease_t;
typedef struct cor_reclaim cor_reclaim_t;

typedef struct cor_context_options {
    uint32_t size;
    uint32_t abi_version;
    uint64_t required_features;
    const char* endpoint;           /* Optional System socket; NULL uses env/process discovery. */
} cor_context_options_t;

typedef struct cor_engine_options {
    uint32_t size;
    uint32_t abi_version;
    const char* label;              /* Nonempty UTF-8; copied by register. */
    const int32_t* device_ids;       /* Physical process-wide IDs; copied. */
    uint32_t device_count;          /* Nonempty, distinct, nonnegative IDs. */
    uint32_t weight;                /* Positive relative share of held time. */
    uint32_t tenancy;
    uint32_t reserved;              /* Must be zero. */
    uint64_t capabilities;
} cor_engine_options_t;

typedef struct cor_engine_stats {
    uint32_t size;
    uint32_t abi_version;
    uint64_t active_work;
    uint64_t grants;
    uint64_t held_ns;               /* Completed leases, wall-clock ns. */
    uint64_t waited_ns;             /* Successful acquisitions only. */
    uint32_t waiting;
    uint32_t holding;
    uint32_t weight;
    uint32_t tenancy;
    uint64_t capabilities;
} cor_engine_stats_t;

uint32_t cor_abi_version(void);
uint64_t cor_features(void);

/*
 * NULL options attaches to the process runtime, or CORBY_ENDPOINT when set.
 * Never starts a runtime. Missing/disconnected runtime returns UNAVAILABLE.
 * The original eight/16-byte option prefixes remain accepted. Endpoint is an
 * appended field, read only when size covers it. Outputs are NULL on failure.
 * Each predictor statically links its PRIVATE client implementation.
 */
cor_status_t cor_context_create(
    const cor_context_options_t* options,
    cor_context_t** result
);
cor_status_t cor_context_destroy(cor_context_t* context);

/*
 * Supervisor access needs no engine-owned handle. IDs are monotonically
 * allocated, never reused, and valid only in this runtime incarnation.
 * Enumeration is a snapshot: retry BUSY with the returned needed capacity;
 * a registration disappearing between calls returns NOT_FOUND. Observing an
 * ID never prolongs a registration or its dedicated reservation.
 */
cor_status_t cor_context_engine_ids(
    const cor_context_t* context,
    uint64_t* ids,
    uint32_t capacity,
    uint32_t* needed
);
cor_status_t cor_context_engine_stats(
    const cor_context_t* context,
    uint64_t id,
    cor_engine_stats_t* result
);
cor_status_t cor_context_engine_devices(
    const cor_context_t* context,
    uint64_t id,
    int32_t* devices,
    uint32_t capacity,
    uint32_t* needed
);
cor_status_t cor_context_engine_set_weight(
    const cor_context_t* context,
    uint64_t id,
    uint32_t weight
);
cor_status_t cor_context_engine_label(
    const cor_context_t* context,
    uint64_t id,
    char* buffer,
    uint32_t capacity,
    uint32_t* needed
);

/*
 * Devices are assigned at registration and never changed per inference call.
 * At most one dedicated registration may reserve a physical device. Shared
 * peers may coexist only if every overlapping engine promises cooperative
 * yielding. These capabilities are promises made by the embedding engine,
 * not something Corby can verify by inspecting GPU execution.
 */
cor_status_t cor_engine_register(
    const cor_context_t* context,
    const cor_engine_options_t* options,
    cor_engine_t** result
);

/*
 * Retire rejects new work; existing work may finish or be cancelled.
 */
cor_status_t cor_engine_retire(const cor_engine_t* engine);

/*
 * BUSY leaves the handle valid. Successful unregister consumes it.
 */
cor_status_t cor_engine_unregister(cor_engine_t* engine);
cor_status_t cor_engine_set_weight(
    const cor_engine_t* engine,
    uint32_t weight
);

/*
 * Initialize size and abi_version. Writes only the caller's known prefix;
 * the returned size is the implementation's known structure size.
 */
cor_status_t cor_engine_stats(
    const cor_engine_t* engine,
    cor_engine_stats_t* result
);

/*
 * needed includes the terminating NUL. NULL buffer with capacity=0 queries it.
 * A short non-NULL buffer returns INVALID_ARGUMENT without copying.
 */
cor_status_t cor_engine_label(
    const cor_engine_t* engine,
    char* buffer,
    uint32_t capacity,
    uint32_t* needed
);

/*
 * Begin before dispatch queueing. Dedicated work blocks shared admission on
 * all of its devices, including gaps between leases. End after execution and
 * stream completion, including cancellation cleanup. Multiple work scopes
 * may exist per engine; only one lease or acquire may be outstanding.
 */
cor_status_t cor_work_begin(
    const cor_engine_t* engine,
    cor_work_t** result
);

/*
 * Sticky cancellation wakes a blocked acquire and requests a holder to yield.
 * It NEVER releases a lease or clears dedicated demand automatically.
 */
cor_status_t cor_work_cancel(const cor_work_t* work);

/*
 * BUSY while this work is acquiring/holding; success consumes the handle.
 */
cor_status_t cor_work_end(cor_work_t* work);

/*
 * Acquires the complete device group atomically. Zero timeout tries once;
 * UINT64_MAX waits indefinitely; other values are a relative monotonic ns
 * budget. Timeout/cancellation leave no queued request or partial lease.
 * Never wait on another engine's progress while holding a lease.
 */
cor_status_t cor_lease_acquire(
    const cor_work_t* work,
    uint64_t timeout_ns,
    cor_lease_t** result
);

/*
 * min_hold_ns applies to fairness, never dedicated demand or cancellation.
 * On error result is true; callers must stop new GPU admission.
 */
cor_status_t cor_lease_should_yield(
    const cor_lease_t* lease,
    uint64_t min_hold_ns,
    uint32_t* result
);

/*
 * Granted COMPUTE: caller MUST drain all GPU work on all devices first.
 * MEMORY: finish allocation calls first. PENDING: cancel the queued acquisition.
 * Failed acquisitions: discard the handle. Successful release consumes it.
 * To yield, drain, release, then acquire again using the same work.
 */
cor_status_t cor_lease_release(cor_lease_t* lease);


/* Optional ABI-v1 extensions. Unknown flags/intents are rejected. */
typedef struct cor_work_options {
    uint32_t size;
    uint32_t abi_version;
    uint32_t resource;
    uint32_t intent;
} cor_work_options_t;

typedef struct cor_acquire_options {
    uint32_t size;
    uint32_t abi_version;
    uint32_t flags;
    uint32_t reserved;
    uint64_t timeout_ns;
} cor_acquire_options_t;

typedef struct cor_memory_requirement {
    int32_t device_id;
    uint32_t reserved;
    uint64_t bytes;
} cor_memory_requirement_t;

typedef struct cor_memory_options {
    uint32_t size;
    uint32_t abi_version;
    const cor_memory_requirement_t* requirements;
    uint32_t requirement_count;
    uint32_t reserved;
    uint64_t timeout_ns;
} cor_memory_options_t;

typedef struct cor_memory_progress {
    uint32_t size;
    uint32_t abi_version;
    uint32_t pending;
    uint32_t completed;
    uint32_t failed;
    cor_status_t status;            /* PENDING until all acknowledge, or deadline. */
    uint64_t released_bytes;        /* Telemetry, never proof an allocation fits. */
} cor_memory_progress_t;

typedef struct cor_memory_release_report {
    uint32_t size;
    uint32_t abi_version;
    uint64_t released_bytes;
} cor_memory_release_report_t;

typedef void (*cor_lease_acquisition_fn)(cor_lease_t* lease, void* context);
typedef void (*cor_memory_handler_fn)(cor_reclaim_t* request, void* context);
typedef void (*cor_memory_progress_fn)(
    cor_lease_t* lease,
    const cor_memory_progress_t* progress,
    void* context
);

/* NULL options retains compute/required work. Memory initialization intent
 * suppresses overlapping opportunistic memory growth without reserving bytes.
 * One pending/held lease per engine PER resource; both domains are independent. */
cor_status_t cor_work_begin_ex(
    const cor_engine_t* engine,
    const cor_work_options_t* options,
    cor_work_t** result
);

/* Blocking success returns a granted lease. ASYNC success accepts a request
 * and returns an opaque pending-or-terminal handle. Never treats PENDING as
 * permission. timeout_ns covers queueing; zero tries once. NULL = blocking forever. */
cor_status_t cor_lease_acquire_ex(
    const cor_work_t* work,
    const cor_acquire_options_t* options,
    cor_lease_t** result
);
cor_status_t cor_lease_acquired(const cor_lease_t* lease);

/* One handler, delivered exactly once on success OR failure, including late
 * registration. May run inline when already complete; otherwise on a Corby
 * completion thread. Enqueue work and return promptly; never block the runtime.
 * Callbacks execute outside scheduler locks and must not throw across C.
 * Destruction must not race registration. Release returns BUSY during delivery;
 * successful release fences/suppresses callbacks not yet executing. */
cor_status_t cor_lease_register_handler(
    cor_lease_t* lease,
    cor_lease_acquisition_fn handler,
    void* context
);

/* Notification only: hand the owning request to the engine's safe-boundary
 * executor. NULL disables future delivery; BUSY if a callback is executing.
 * Requests already delivered remain valid and must still be completed. */
cor_status_t cor_engine_set_memory_handler(
    const cor_engine_t* engine,
    cor_memory_handler_fn handler,
    void* context
);

/* Granted MEMORY lease only, one reclamation operation per lease. Copies the
 * actual per-device allocation requirements. Pushes progress immediately after
 * acknowledgements, potentially coalescing concurrent completions. The final
 * progress has status != PENDING. Release ends interest without invalidating
 * recipients' tickets. Registered peers lacking a handler report UNSUPPORTED. */
cor_status_t cor_lease_reclaim_memory(
    cor_lease_t* lease,
    const cor_memory_options_t* options,
    cor_memory_progress_fn handler,
    void* context
);

cor_status_t cor_reclaim_requirements(
    const cor_reclaim_t* request,
    cor_memory_requirement_t* requirements,
    uint32_t capacity,
    uint32_t* needed
);

/* Acknowledge only AFTER backing is released and device work is synchronized.
 * Success consumes the ticket, including when the requester already cancelled.
 * NULL report means no byte telemetry. Zero bytes is a valid successful attempt.
 * Never pass PENDING as an outcome. Completion is not a memory lease release. */
cor_status_t cor_reclaim_complete(
    cor_reclaim_t* request,
    cor_status_t outcome,
    const cor_memory_release_report_t* report
);

/* Thread contract: non-consuming calls may run concurrently. A caller must
 * synchronize destruction against all uses of the same handle, and serialize
 * acquire calls for an engine/resource. Cancellation may run concurrently with acquire.
 * Destroy/end/unregister/release accept NULL. Other pointers must be live,
 * correctly aligned and valid for the documented sizes. Handles cannot be
 * copied, forged, used after consumption, or shared across processes.
 * Context destruction does not invalidate children. Corby contains panics;
 * INTERNAL is fatal to that coordinator's use, never a free-run fallback. */

#ifdef __cplusplus
}
#endif
