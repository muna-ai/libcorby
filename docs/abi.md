# Compatibility boundaries

The stable predictor contract is the `cor_*` C API. Predictors statically link
`Corby::client`; hosts start a Rust `Runtime` independently. A client contains
transport and local handles, never scheduling state or policy. Updating the
host updates scheduling for existing compatible predictors after host restart.
Live replacement of a provider with outstanding objects is unsupported.

## C and direct dispatch

C scalar widths, calling conventions, exported API names and existing field
offsets are fixed within ABI major 1. Public structs begin with size and ABI
major. Original minimum prefixes remain accepted; additions must be appended,
read only when fully covered, and given explicit defaults when absent. Required
features are negotiated at context attachment. Unknown required features,
capability/reserved bits or ABI majors fail closed. Snapshot writes never exceed
the caller's size, and report the runtime's known size. Callers reinitialize
size before reusing an older short snapshot buffer.

`dispatch.h` is an immutable native C table shared only within one process.
Its original required prefix ends at lease_release; optional entries may be
appended, never reordered. New stubs must preserve that minimum and negotiate
features before accessing additional entries. All pointed-to code stays loaded
for the host process's lifetime. Only the provider interprets backend handles.

The process rendezvous is `/tmp/corby-<uid>/process-<pid>.provider`, beneath a
0700 owned directory. An owned 0600 file is exclusively flocked for the runtime
lifetime and contains `CORBY1 <pid> <hex-table-address>`. Clients verify owner,
mode, regular-file identity, owning PID, a live exclusive lock and the table's
ABI/size. A stale file without a live lock returns Unavailable before dereference.
Pointers in this file are deliberately process-local, never used for IPC. A
runtime removes only its own publication/socket identity. The Rust singleton
and OS lock reject duplicate owners, including separately linked providers.

Client implementation symbols are hidden in predictor DSOs so RTLD_GLOBAL does
not interpose one stub over another. The backend's C dispatch callbacks are not
exported under predictor `cor_*` names. The optional host library exports only
its distinct `cor_runtime_*` startup/lifecycle interface.

## IPC

The System transport uses the separate versioned message protocol, with no C,
Rust, pthread or STL object layout on the wire. The host owns all handles and
maps session-local IDs to backend objects. Supervisor registration IDs refer to
the runtime incarnation, are not pointers, and do not retain registrations.
A stale ID returns NotFound. Different incarnations never share sessions.

Same-user socket credentials and unpredictable session tokens authenticate
connections. One control connection carries asynchronous yield notifications;
RPCs have independent streams so a pending acquire never obstructs cancel or
release. Current limits are 64 KiB per frame, 4096 engines/devices per group,
4096-byte UTF-8 labels, and 128 concurrent server connections (including control
connections). IPC request/reply connection setup is not performance tuned.

Do not reclaim permission when a client connection closes. Cancel waits; retain
uncertain holders and their demand until external fencing and GPU cleanup.
The current implementation quarantines those registrations until runtime/worker
group restart. Runtime shutdown retires admission, reports Busy while any engine
remains, and never revokes a held lease. A missing runtime never becomes a
per-predictor fallback or an inactive lease.

## Ownership and execution

A successful consuming call destroys its handle; Busy leaves it valid. NULL
teardown is allowed. Other pointers must be valid, aligned and cover the stated
sizes. Synchronize destruction with every use of that handle. Cancellation may
race acquire; work_end may not. Multiple work scopes per engine are supported,
but only one acquisition or lease may be outstanding. Rust frontend lifetimes
prevent normal work/lease parent destruction; C/C++ callers obey this explicitly.

Release means every associated device stream has drained. Corby cannot verify
this and does not depend on CUDA. TP engines acquire/release the whole group
from one coordinator rank, synchronizing ranks before release and after grant.
Never wait on another engine's progress while holding a lease. Cooperative
capabilities are engine promises; a client that ignores yield requests can
prevent progress. No forced GPU permission reclamation is attempted.

Core mutex poisoning and panics fail closed. Panics are contained at C/RPC
boundaries; ordinary Busy, Timeout, Cancelled and Unsupported statuses are
recoverable. Allocation exhaustion/process-fatal failures are outside recovery.
Internal transport failures preserve uncertain ownership. If the runtime dies,
clients must stop new GPU admission and drain; a supervisor fences old workers
before a replacement can grant permission safely.
