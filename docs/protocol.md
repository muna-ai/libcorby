# System protocol v1

All integers are little-endian fixed-width values. Every stream frame starts
with four u32 values: magic `0x434f5242`, wire major `1`, operation, payload bytes.
Payloads are limited to 65536 bytes before allocation. Unknown wire majors,
invalid magic, oversized or truncated frames close the request connection.
Commands reject missing or trailing fields. These layouts are protocol bytes,
not mapped native structs. Changing an existing command's schema requires a
negotiated new command/version; append-only APIs do not imply arbitrary trailing
wire fields are accepted. Existing operations and meanings remain supported.

Responses repeat the operation and start their payload with i32 status. A zero
status is followed by the operation's result; an error has no result. A string
is u32 byte count followed by UTF-8 bytes, without a terminator. Labels must be
nonempty, at most 4096 bytes and contain no NUL. Device arrays are u32 count then
i32 IDs. Registration ID arrays are u32 count then u64 IDs.

## Sessions

The client opens a control stream and sends operation 0 with u64 required
features. A successful response contains u64 runtime features and a random
16-byte session capability. Both sides retain the stream. Only same-user peers
are accepted (SO_PEERCRED on Linux, getpeereid on macOS); socket permissions are
0600. The capability is private to this session/runtime incarnation.

Subsequent RPC streams send one command and receive one response. Their payload
starts with the 16-byte session capability, followed by command arguments. This
allows multiple outstanding operations without serializing behind a blocked
acquire. Unknown/closed sessions return Unavailable. Objects belong to their
session, with u64 IDs never reused within that session. Registration IDs used
by supervisor commands are separate backend IDs.

| Op | Operation | Arguments after session capability | Result |
|---|---|---|---|
| 3 | Enumerate registrations | none | registration ID array |
| 4 | Registration stats | u64 registration | stats |
| 5 | Registration devices | u64 registration | device array |
| 6 | Set registration weight | u64 registration, u32 weight | none |
| 7 | Registration label | u64 registration | string |
| 8 | Register engine | string label, device array, u32 weight, u32 tenancy, u64 capabilities | u64 engine object |
| 9 | Retire engine | u64 engine | none |
| 10 | Unregister engine | u64 engine | none |
| 11 | Set engine weight | u64 engine, u32 weight | none |
| 12 | Engine stats | u64 engine | stats |
| 13 | Engine label | u64 engine | string |
| 14 | Begin work | u64 engine | u64 work |
| 15 | Cancel work | u64 work | none |
| 16 | End work | u64 work | none |
| 17 | Acquire group | u64 work, u64 relative timeout ns | u64 lease |
| 18 | Query yield | u64 lease, u64 minimum hold ns | u32 boolean |
| 19 | Release lease | u64 lease | none |

Operations 1/2 are reserved for local context lifecycle; System contexts use the
control stream. Destroying a local context does not close the System session
until all its child handles are gone. Consuming commands 10/16/19 are idempotent
for already absent IDs within the session. Clients do not transparently retry
uncertain operations or move their handles onto another session/runtime.

Stats are exactly the original 64-byte snapshot: u32 known size and ABI, then
u64 active work/grants/held ns/waited ns, then u32 waiting/holding/weight/tenancy,
then u64 capabilities. Decode fields explicitly rather than casting bytes to a
native struct. A future extension needs a negotiated operation or wire schema.

## Yield push and recovery

Operation 100 on the control stream is an unsolicited notification, with u64
lease and u32 flags (no status prefix). Flag 1 indicates fair yielding, flag 2
indicates urgent cancellation/dedicated demand, and zero clears the request.
The server samples held leases approximately every millisecond and sends changes.
The client caches flags in local atomics. Minimum-hold filtering applies to fair
yielding only; urgent flags bypass it. Transport loss makes should_yield return
Unavailable with a true output, preventing new admission by a compliant engine.

Notification observation must not extend lease ownership past a release reply.
Session/scheduler locks must not be held while sending to slow clients. Closing
the control stream cancels waits and quarantines uncertain held leases. It does
not prove GPU completion. No lease expiry, disconnect-triggered regrant, or
transparent runtime failover exists. A supervisor must fence old workers and
confirm cleanup before starting replacement coordination.
