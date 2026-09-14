# Communication logging and parent-owned child tracing

Status: proposal, 2026-09-14. Parent tracing is not implemented by this document.
Operator logging is a separate implementation slice. This proposal supersedes
sibling isolation as the subject of issue #63; it does not change group routing.

## Decisions

Child tracing defaults to **off**, is selected by the parent, and requires no
child approval. Operator logging and parent tracing are independent controls:
neither enables the other. Traces describe Sessionbus traffic, not arbitrary
shell/network activity, native prompts outside Sessionbus, or model reasoning.

Use one structured event vocabulary and one rotated JSONL stream per daemon.
Sessions, owners, groups and runs are indexed fields or query filters, not separate
files. A group broadcast overlaps several sessions and a session can belong to
several groups; splitting files would duplicate messages and complicate ordering,
retention and access. A hub has its own optional log, never enabled implicitly
by a host's setting. Cross-host records correlate; they do not establish a global
clock order.

Sending a copy to the parent is a useful interface, but ordinary `message.send`
is the wrong carrier: it can wake an idle-run parent, inject unsolicited context,
misattribute an inbound copy to the child, recurse, and disappear during outages.
Represent the copy as a trace event. The retained stream is the record; live
notifications are an optional convenience. Do not resend the original message
when a trace notification is lost.

## What an event means

Each record has a schema version, daemon incarnation, monotonic per-incarnation
sequence, UTC observation time, host and event kind. Specific records contain
message_id, delivery_id, requested labels/group, resolved source/target identities,
session_id, run_id, and relevant groups as captured at that event's boundary.
Never serialize federation secrets, owner authorization tokens, environment,
arbitrary peer info, native arguments or raw protocol frames.

Record send requests, individual dispatches or rejections, returned receipts,
run dispatch/terminal/ack, lane open/close/forget, connection loss, and recording
boundaries. Content mode additionally records the explicit communication body
and, where supported, Run input/result text. Metadata mode omits those fields.
Receipt disposition is explicitly a product report, or a daemon-generated loss
classification. A send copy and its later receipt are separate correlated events.
`written` does not prove consumption, `queued_for_next_turn` does not identify a
future consuming run, and `no_receipt` remains uncertain. The present wire cannot
reconstruct a native consumption event it never received; such an extension
would need an identity-bound adapter witness rather than a timestamp guess.

## Operator logging: first implementation slice

Proposed service configuration:

```
SESSIONBUS_COMMS_LOG=off       # off | metadata | content
# SESSIONBUS_COMMS_LOG_DIR=/absolute/private/directory
```

The directory defaults beneath the daemon's XDG state directory. Generated
service.env documents the option and leaves it off. Existing installations keep
their explicit settings. Size/count limits bound disk retention. Directory mode
0700 and files 0600 keep content out of general service output; diagnostics report
logging failures without echoing message bodies. A dedicated log is portable
across systemd and launchd; operational errors can still use their service logs.

One writer serializes records. A byte-bounded queue keeps disk writes out of
routing locks and protocol loops. Pressure or write failure must be visible as
missing sequence ranges/loss counters, never silently represented as a complete
trace. Logging failure does not change delivery receipts or replay traffic.
Startup configuration/path errors are reported before serving; later failures
are reported operationally. Close joins the writer and attempts to flush.

This is diagnostic logging, not a transaction log or an exactly-once ledger.
Buffered records can be lost on crash, including an unwritten gap marker. A clean
stop/checkpoint can bound what was flushed; a missing clean end, retention expiry
or gap makes completeness unknown. A log cannot prove absence of communications
outside Sessionbus or an interval it did not cover.

## One copy when two traced children talk

For a common parent, a logical send has one message header and one content body,
identified by message_id. matched_children contains the union of traced source
and recipient children; matching both endpoints does not create another copy.
Dispatch and receipt records refer to that header by message_id/delivery_id and
never repeat its body. Two deliberate sends with identical text have different
message IDs and remain distinct. Group fan-out has separate delivery records per
recipient, not another content copy for each recipient.

Pagination, catch-up and live push share the same logical IDs and dedup contract;
normal continuation must not repeat content at a page or host boundary. An
explicit reread of an older cursor can naturally return the same existing event.
Late participant/receipt information is a metadata update, not another content
message. Event-time parent policy still controls disclosure: content is permitted
if at least one matched child was content-enabled; otherwise it is stripped.
Do not disclose untraced recipient details just because another recipient is traced.

Source and destination daemon logs can each retain their own physical observation
for troubleshooting. The parent's federated view coalesces those observations by
logical ID, while retaining per-host cursors and gap information. Protocol 2 should
carry delivery_id in message.deliver (it currently exists only in sender results)
so adapter, destination and parent records can correlate without guessing from
text or timestamps. This is part of the acceptance tests, not an optional UI tidy-up.

## Parent tracing: proposed protocol extension

Expose `trace: off | events | content` at child spawn/resume, defaulting to off.
An existing child can be changed with `trace.configure {session_id, mode}` by its
trace owner. Configuration returns the effective policy and observation boundary.
No retroactive exposure of operator-only records; enabling affects subsequent
events, disabling stops subsequent recording for that parent. Events already
recorded retain their event-time policy. Resuming a lane does not silently adopt
a former parent's tracing choice.

Bind a separate trace owner when the creator opts in, independently of the lane's
persistent/notify/auto-close policies. Persistent lanes can be traced without
making their lifetime depend on the parent. An unrelated visible peer or sibling
cannot configure tracing merely through shared group membership. The tracing
scope is direct child traffic; do not recursively copy trace notifications or
trace-read responses up a hierarchy.

`trace.read {session_id?, cursor?, limit?}` returns bounded event pages, next cursor,
retention/gap information and the known recording interval. The daemon checks the
caller's live ownership epoch against the event's trace owner. Group and session
selectors narrow an authorized result; they do not grant access. A child being
forgotten does not erase an event while its parent and retention remain valid.

Optional `trace.event` pushes use a dedicated SDK trace handler, never Deliver or
Run. An attachment must advertise support before receiving them. A product may
render or store these notifications, but they do not become model context or
start inference by default. A missed push is recovered with trace.read rather
than delivery retries. A parent that wants model-visible copies can make that an
explicit product/policy choice, with bounded volume.

When operator content logging and parent events-only tracing overlap, the stored
row may contain content, but the parent projection MUST strip it. The parent's
maximum disclosure is its policy at event time, regardless of the operator's
setting or a later change to content mode. Operator-only events never become
parent-visible. Store a non-authorizing owner epoch in records, never the real
runtime owner token.

## Restart, federation and compatibility

A cursor is (daemon incarnation, sequence); do not pretend clocks produce global
ordering. Remote child queries reach the child's daemon, which enforces the
trace-owner check using authenticated federation attachment/lifetime context.
Old remote hosts must report unsupported tracing, not an empty complete stream.

Current owner lifetimes are not durably authenticated. In the initial design,
parent API access lasts only for the authenticated live ownership epoch; after
its loss or daemon restart, retained files are operator-only. Claiming the same
session ID must not unlock them. Durable parent catch-up across that boundary
requires a separate resumable ownership/capability design. Do not hide this
limitation behind a group filter or a claimed owner_session_id string.

The current protocol's hello and request schemas are closed. Adding
supports_trace to old hello frames is not backward compatible. Plan an explicit
protocol-2 capability boundary: new daemons retain protocol-1 service, while
trace-aware attachments negotiate protocol 2. Old daemons currently reject an
explicit protocol-2 hello with generic invalid_hello and close the connection.
The client must explain that protocol-2 negotiation was rejected (including the
possibility of an older daemon), not invent a specific server error or silently
retry as ordinary message delivery. The coordinated change includes both SDKs,
federation validation, tool declarations and their drift tests, skills, and
product notification handling. Trace policy/queries are advertised only where
supported.

## Validation before implementation is accepted

- Default-off produces no parent trace; only its creator can enable/disable it,
  including while running and for persistent children.
- Parent events-only never receives operator-retained content; siblings and
  same-ID replacement/restart cannot obtain historic authority.
- Local/federated fan-out preserves correlation; loss before/after dispatch and
  native uncertainty keep distinct truthful labels.
- Traces neither wake runs nor recursively trace themselves; saturated/offline
  parents do not delay or reorder ordinary traffic.
- Rotation, retention, overflow, disk failure, abrupt restart and cursor expiry
  explicitly limit completeness; forget does not erase retained observations.
- Mixed protocol versions reject unsupported tracing; no new field is silently
  sent through an old closed schema.

## Related protocol-2 improvement: uncertain delivery

Use an explicit `uncertain` disposition for missing receipts or loss after
submission, rather than the misleading `rejected/no_receipt` combination.
Reserve `rejected` for observed refusal or proven failure before submission.
Keep `written` as its existing local-write claim, not consumption. A protocol-2
reason vocabulary should distinguish known non-submission from post-dispatch
loss without guessing which native turn consumed a message. The compatible
protocol-1 fix can distinguish proven `not_submitted` now, while older peers
still retain the documented `no_receipt` uncertainty. This amendment requires
coordinated daemon, SDK, federation and tool validators; it is proposed here,
not silently added to protocol 1.
