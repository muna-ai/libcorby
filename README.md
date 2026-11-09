# Corby

Corby coordinates cooperative execution across engines sharing physical devices.
A **Rust runtime** owns all scheduling state. Each predictor statically links a
small **C++ client**, which discovers the runtime and forwards the `cor_*` C API.
Neither the client build nor predictor compilation requires Rust. Function and
predictor-loading infrastructure have no role in runtime initialization.

## Start the runtime from Rust

```rust
use corby::{Runtime, RuntimeOptions, RuntimeScope, Tenancy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = Runtime::start(RuntimeOptions::default())?;
    let supervisor = runtime.supervisor();
    // Load predictors independently. Their C++ stubs attach automatically.

    let engine = supervisor.register("example", &[0, 1], 1, Tenancy::Shared, true)?;
    let work = engine.begin()?;
    let lease = work.acquire(None)?;
    // Execute native device work; drain ALL streams before dropping the lease.
    drop(lease);
    drop(work);
    drop(engine);
    runtime.shutdown()?;
    Ok(())
}
```

The host may also supervise an engine registered by a predictor. After finding
its ID through `engine_ids`, `engine_label` and `engine_devices`, set its tenancy
with `set_engine_tenancy(id, Tenancy::Dedicated)` before admitting requests.
`begin_engine(id)` holds invocation-wide work intent on that same registration;
retain it until native execution and stream cleanup finish, including after a
caller disconnects. The returned work owns the registration internally. Tenancy
changes require an idle engine and validate all overlapping device groups.

`RuntimeScope::Process` is the default. A process-private rendezvous publishes
an immutable C dispatch table. Stubs discover it once and thereafter make direct
calls; Rust structs, locks, allocator state and scheduling policy stay private.

For clients in other processes on the same machine:

```rust
let mut runtime = corby::Runtime::start(corby::RuntimeOptions {
    scope: corby::RuntimeScope::System,
    endpoint: Some("/run/muna/corby.sock".into()),
})?;
```

System scope also permits direct clients in the hosting process. Other workers
attach using the endpoint in `cor_context_options_t`, the C++ wrapper's second
`Context::create` argument, or `CORBY_ENDPOINT`. An explicit endpoint takes
precedence over the environment. The default System endpoint is
`/tmp/corby-<uid>/system.sock`. The application must create any custom parent
endpoint directory. Existing socket paths are never overwritten automatically.
One runtime may run per hosting process; independent processes can expose
separate named System domains. Engines sharing devices must use the same domain.

Both transports enter the same backend. System mode uses bounded, versioned
Unix-domain socket messages, opaque session/object IDs and a separate push
channel for yield notifications. A blocked acquire does not block cancellation,
metadata queries or another engine's release. Yield checks in predictors read
local atomics instead of making a socket round trip. The initial notification
service samples held leases roughly every millisecond; OS scheduling, GPU safe
points and drain latency also contribute to handoff latency. This is cooperative
scheduling, not hard real-time preemption.

## Predictor consumption: CMake only

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
cmake --install build --prefix "$PWD/build/install"
```

The default build does not find or invoke Cargo. Installed usage:

```cmake
find_package(Corby CONFIG REQUIRED)
target_link_libraries(my_predictor PRIVATE Corby::client)
```

The static client is position independent, and its implementation symbols are
hidden inside the final predictor DSO. Each predictor can carry its own stub
without exporting competing `cor_*` definitions. All stubs consume the selected
runtime; none embed a scheduler or create a fallback runtime.

The public C API is in [corby.h](include/corby/corby.h). Move-only C++ wrappers
are in [corby.hpp](include/corby/corby.hpp):

```cpp
auto context = corby::Context::create(); // Or create(0, "/run/muna/corby.sock").
// Check each Result/status before using its value.
const std::array<int32_t, 2> devices{ 0, 1 };
auto engine = context.value.register_engine("my-engine", devices);
auto work = engine.value.begin();
auto lease = work.value.acquire();
// Execute and drain every associated device stream before lease destruction.
```

Only hosting applications build the Rust backend. Rust hosts depend on this
Cargo package directly; `corby::Runtime` and its safe supervisor frontend use
ordinary Rust ownership, with no C++ build dependency. C/C++ hosts can optionally
build the runtime and link `Corby::runtime`, using
[runtime.h](include/corby/runtime.h) to start, shut down and destroy it:

```sh
cmake -S . -B build-host -DCORBY_BUILD_RUNTIME=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-host --parallel
ctest --test-dir build-host --output-on-failure
```

No external Rust or C++ packages are needed. Rust 1.82+, CMake 3.20+ and C11/C++20
compilers are supported requirements. Current platform support is Linux/macOS.

## Work and scheduling

- An **engine** registers its fixed device group, label, weight, tenancy and
  cooperative-yield capability. IDs are host-assigned physical identities,
  **not process-local CUDA ordinals**. Every process must use the host's mapping.
- **Work** represents queued or executing demand. Begin before dispatch queueing;
  end only after native execution and stream cleanup. Multiple requests can
  contribute work to one engine.
- A **lease** grants the complete group atomically. One acquire or held lease may
  be outstanding per engine. Never wait on another engine's progress while held.
- Shared contenders use stride scheduling charged by actual wall-clock held
  time divided by weight. Returning engines do not accumulate idle credit.
  Queue precedence prevents single-device arrivals starving an older group.
- At most one dedicated engine may reserve a device. Its active work blocks
  shared admission even between leases and requests existing holders to yield.
  While it has no work, shared peers can borrow the devices. Overlapping engines
  must advertise cooperative yielding before dedicated admission succeeds.

Devices are selected at registration, never per inference request. Corby does
not load models, choose GPUs, perform CUDA synchronization, or coordinate TP
rank barriers. The host/engine implements these around Corby's group lease.

Cancellation is sticky and wakes waiters. It requests a holder to yield but
never releases permission or clears its work demand automatically. Timeout and
cancellation leave no queued request or partial group. For a cooperative yield,
drain all streams, release, then acquire again on the same work scope.

## Shutdown and failures

Call `runtime.shutdown()` after engines finish. It first stops admission and
retires registrations. `Busy` preserves the runtime so existing work can finish
and callers can retry shutdown. Dropping a busy Rust runtime leaves its backend
pinned and retiring rather than invalidating callbacks or revoking leases;
explicitly drain and shut down before dropping it. A dropped busy owner requires
host restart to replace. A second start in that process returns Conflict.

A broken IPC control connection cancels outstanding demand/waits. Registrations
without held leases are cleaned up as in-flight requests settle. **Uncertain
held leases and their demand/reservations remain quarantined.** Socket closure,
timeout or process death is not treated as proof of GPU stream completion.
Quarantine intentionally blocks conflicting grants and clean shutdown. There is
no automatic fencing/reclamation API yet; the supervisor must stop/fence the
worker group and restart the runtime after GPU cleanup. Runtime restart while
old workers are still executing is unsupported. Clients never silently attach
existing handles to a new incarnation.

Keep the runtime's executable/library code loaded for the hosting process's
lifetime; stubs cache function pointers. Forked workers must exec and attach to
a System endpoint; inherited runtime/client handles cannot be used after fork.
The C++ client keeps a context/session alive until its child handles are gone.
Handle destruction must not race other calls using that handle. GPU drains must
also happen on exception, cancellation, and Rust unwinding paths before a lease
is released. Internal errors never enable free-running device work.

## Validation and compatibility

```sh
cargo test --locked --offline
cargo clippy --locked --offline --all-targets -- -D warnings
cargo fmt --all -- --check
```

The host-enabled native suite tests local C/C++ clients, independent statically
linked DSOs, and an exec'd worker sharing one runtime. It covers blocked IPC
acquire cancellation, finite timeout cleanup, metadata/policy access, dedicated
push notifications, synchronous release ordering and crash quarantine. Rust
tests cover the scheduler, runtime lifecycle and bounded message decoding.
Socket tests need local Unix-socket binding and process spawning permissions.
These are CPU tests; real GPU drain/TP-barrier validation remains with engines.

The [ABI contract](docs/abi.md) covers stable C prefixes, dispatch and lifetimes.
The [IPC protocol](docs/protocol.md) documents its framing and evolution rules.
This project does not yet modify NanoSGL, muna-server, Function or the compiler.
