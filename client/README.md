# Static client implementation

`Corby::client` implements the public C API in `include/corby/corby.h`.
The files below are private implementation details and are not installed.

- `api/context.cpp`: feature queries, context creation and endpoint selection.
- `api/engine.cpp`: engine registration, lifecycle and metadata.
- `api/work.cpp`: work lifecycle and cancellation.
- `api/lease.cpp`: acquisition, local yield checks and release.
- `api/supervisor.cpp`: inspection and management through registration IDs.
- `api/handles.hpp`: opaque C handle definitions and shared connection ownership.
- `api/support.*`: C exception boundaries, ABI prefix checks and result copying.
- `transport/discovery.*`: process-provider validation and dispatch-table discovery.
- `transport/socket.*`: Unix socket connection setup and descriptor ownership.
- `transport/protocol.*`: binary encoding, bounded framing and stream I/O.
- `transport/connection.*`: connection lifetime, session RPC and yield notifications.

API entry points forward through the native dispatch table for Process clients
or encode RPCs for System clients. Scheduling policy remains in the Rust runtime.
