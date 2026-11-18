# Memory coordination

Memory leases serialize **backing-memory growth** over an engine's device group.
Compute leases serialize execution. These are independent resource domains: a
new engine holding a memory lease does not stop residents finishing compute,
synchronizing streams, or releasing memory.

Corby does not own a CUDA allocator, estimate model size, track every library
allocation, or decide where an engine can safely reclaim memory. The engine
supplies actual allocation requirements and acknowledges releases at its own
safe boundary. CUDA allocation success remains the source of truth.

## Allocation lifecycle

1. Begin `Memory / Initialization` work around engine initialization. This is
   intent, not a reservation or a byte budget; initializers can overlap their
   I/O, host preparation and compute.
2. Acquire a memory lease for an allocation attempt. **All cooperating backing
   growth**, including VMM page mapping and cache regrowth, uses this domain.
3. Try the allocation. On OOM, call `reclaim_memory` with that allocation's
   actual per-device requirements while retaining the lease.
4. Each overlapping resident receives an opaque reclamation ticket. Its
   notification handler enqueues the ticket on its own executor and returns.
   At a safe boundary, synchronize relevant streams, trim reclaimable backing,
   then complete the ticket. Releasing memory does **not** require a memory lease.
5. Corby pushes progress on acknowledgement. Retry after useful progress;
   there is no need to wait for every peer if the allocation already fits.
   Concurrent acknowledgements may coalesce into a single progress snapshot.
6. Release the memory lease when the allocation calls finish. Continue loading
   and acquire another short lease for the next backing allocation. End the
   initialization work when loading finishes.

Keep work scopes longer than leases. Overlapping `Initialization` work prevents
`Opportunistic` cache growth from immediately reclaiming freed headroom between
allocation turns. `Required` growth remains admissible, and initializers still
arbitrate each allocation rather than monopolizing initialization. Compute
weights do not set memory shares; memory turns are charged equally with ticket
ordering for ties. Non-overlapping groups can grow concurrently.

Do not wait for a memory lease while holding compute permission if the memory
owner might need compute progress. When both are needed, use a consistent order
(memory before compute), and release at the engine's established safe boundary.
A callback must not block waiting for another Corby callback. The next NanoSGL
integration will supply its internal executor/safe-boundary policy separately.

## Async acquisition is still a lease

```cpp
auto work = engine.begin(
    corby::Resource::Memory,
    corby::WorkIntent::Initialization
);
// Check every Result/status before using its value.
auto requested = work.value.acquire({
    .timeout_ns = 2'000'000'000,
    .asynchronous = true
});

const auto status = requested.value.acquired();
// COR_PENDING is not permission to allocate. Register a handler or wait using
// the application's executor; no additional acquisition object is needed.
requested.value.register_handler(
    [](cor_lease_t* lease, void* context) {
        const auto outcome = cor_lease_acquired(lease);
        // Notify the engine executor of outcome; return promptly.
    },
    context
);
```

The C functions are `cor_lease_acquire_ex`, `cor_lease_acquired` and
`cor_lease_register_handler`. The acquire call returns `COR_OK` when it accepts
an async request, including an already completed request. Querying the lease
returns `COR_PENDING`, `COR_OK`, or its terminal failure. One handler can be
registered, exactly once on success or failure, including after completion.
Late registration may invoke inline. There is no new thread per acquisition.
Blocking acquisition remains available through both old and extended APIs.

Releasing a pending lease cancels its queue entry; successful release suppresses
callbacks not already executing. A callback in progress makes release return
`COR_BUSY`. It must return before its borrowed context can be destroyed. A
callback can query its lease but should post release to its executor. The safe
Rust frontend also handles dropping a lease from its own callback by deferring
cleanup until delivery returns. Cancellation of a granted lease never revokes
permission: compute must drain, and allocation calls must finish before release.

## Reclamation tickets and progress

```cpp
const cor_memory_requirement_t requirements[]{
    { .device_id = device_id, .reserved = 0, .bytes = allocation_bytes }
};
requested.value.reclaim_memory(
    requirements,
    [](cor_lease_t*, const cor_memory_progress_t* progress, void* context) {
        // Copy the borrowed snapshot onto the engine's executor. Retry there
        // while retaining the memory lease, even if progress->pending > 0.
    },
    context,
    2'000'000'000
);
```

Register a resident handler with `cor_engine_set_memory_handler` or the C++
`Engine::set_memory_handler`. It receives an owning `cor_reclaim_t*`. The engine
can move that pointer into its executor, use `cor_reclaim_requirements` to read
its device subset, and finish with `cor_reclaim_complete`. `corby::Reclaim` is a
move-only C++ wrapper with `requirements()` and `complete(outcome, bytes)`;
destruction of an unfinished ticket reports cancellation.

There is one reclamation operation per granted memory lease. Internal rounds
and participant IDs never escape Corby. Tickets keep the recipient registration
alive, and unregister reports busy while obligations remain. Releasing the
requester's lease stops its progress delivery without invalidating outstanding
tickets; a late recipient acknowledgement still succeeds.

Progress includes pending/completed/failed recipient counts, cumulative released
bytes and an outcome. `COR_PENDING` means more acknowledgements are outstanding.
The terminal outcome is `COR_OK`, the first peer failure, or `COR_TIMEOUT`.
Timeout does not claim that outstanding engines have stopped; they still finish
their tickets. A successful zero-byte release attempt is valid. A peer with no
handler reports `COR_UNSUPPORTED`, and an abandoned Rust/C++ ticket reports
`COR_CANCELLED`. Byte counts are telemetry, **not proof that an allocation fits**.
The allocator must always retry and check its actual result.

## Rust frontend

```rust
use corby::{AcquisitionOptions, MemoryRequirement, Resource, Status, WorkIntent};
use std::sync::mpsc;
use std::time::Duration;

// Resident setup: hand ownership to its executor without doing CUDA work in
// the Corby notification callback. A failed send drops/cancels the ticket.
let (warnings, resident_executor) = mpsc::channel();
resident.set_memory_handler(move |request| { let _ = warnings.send(request); })?;

// In the resident executor, after synchronization and actual backing release:
let request = resident_executor.recv()?;
let requirements = request.requirements();
request.complete(Status::Ok, released_bytes)?;

// New engine, before attempting a backing allocation:
let work = newcomer.begin_ex(Resource::Memory, WorkIntent::Initialization)?;
let lease = work.acquire_ex(AcquisitionOptions {
    timeout: Some(Duration::from_secs(2)),
    asynchronous: true,
})?;
let (completed, executor) = mpsc::channel();
lease.register_handler(move |outcome| { let _ = completed.send(outcome); })?;
// Wait outside Corby callbacks (or bridge the channel onto the engine executor).
assert_eq!(executor.recv()?, Status::Ok);
let (progress, executor) = mpsc::channel();
lease.reclaim_memory(
    &[MemoryRequirement::new(device_id, allocation_bytes)],
    Some(Duration::from_secs(2)),
    move |snapshot| { let _ = progress.send(snapshot); },
)?;
```

## Transports and compatibility

ABI major remains **1**. The original C dispatch prefix and all original compute
operations remain unchanged. New clients negotiate `COR_FEATURE_MEMORY_LEASES`
and `COR_FEATURE_ASYNC_LEASES`; appended table slots are checked before access.
An old runtime returns unsupported for these extensions rather than pretending
to coordinate memory. No shared-memory layout or CUDA type enters the public ABI.

Process clients use direct dispatch. System clients use versioned Unix socket
commands and pushed completion/warning/progress messages. The scheduler sleeps
on state changes/deadlines, and the IPC listener blocks in accept. Reclamation
and acquisition notifications are enqueued directly from transitions; they do
not wait for the legacy compute-yield sampler. Slow notification consumers are
disconnected rather than blocking the scheduler. Callbacks must enqueue work
and return promptly; Corby provides no hard real-time latency guarantee.

CPU-only tests exercise synthetic allocations, partial progress, missing
handlers, deadlines, cancellation, late acknowledgement, independent compute,
and an exec'd C++ client process. They do not validate real CUDA synchronization
or reclamation inside NanoSGL; that is the next integration step.
