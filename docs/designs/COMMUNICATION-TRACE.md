# Communication logging and parent-owned child tracing

Status: revised proposal, 2026-09-14. Parent tracing is not implemented.
Operator logging is a separate implementation already merged in PR #71.
This proposal supersedes sibling isolation as the subject of issue #63;
it does not change group routing.

## Hard requirement: no new persistence

**Parent tracing MUST NOT introduce any new persistence.** This is an acceptance
constraint, not an optimization or a deferred implementation choice.

- No trace database, journal, history files, spool, write-ahead log or disk-backed
  queue in daemons, hubs, SDKs or wrappers.
- No persisted trace policy, subscriber registration, delivery acknowledgement,
  cursor, deduplication state, ownership token or recovery checkpoint. Existing
  durable lane rows must not acquire tracing state.
- No `trace.read`, retained-history API, replay, catch-up or restart recovery.
- Only bounded volatile state is permitted: live policy/ownership, pending
  notifications, correlation/deduplication and loss counters. Bound both bytes
  and event counts; queued work also needs a lifetime bound.
- Trace state is discarded when its live ownership ends or the process restarts.
  A reconnected parent may explicitly configure a new live subscription after
  normal ownership validation; there is no historical access to restore.
- The independently enabled operator JSONL log remains a diagnostic output.
  Tracing must work with it disabled and must never read it, index it, use it as
  a queue, or reconstruct trace state from it. This requirement authorizes no
  additional persistence in that logger either.

A design that requires durable state must be rejected or simplified. Relaxing
this constraint requires a separate explicit owner decision.

## Parent controls and scope

Child tracing defaults to **off**, is selected by the parent, and requires no
child approval. Operator logging and parent tracing are independent controls:
neither enables the other. Traces describe Sessionbus traffic, not arbitrary
shell/network activity, native prompts outside Sessionbus, or model reasoning.

The proposed initial setting at spawn/resume is `trace: off | events | content`.
`trace.configure {session_id, mode}` changes a direct child's live policy, including
while it runs. Configuration returns the effective policy and a live observation
boundary. Enabling applies to subsequent observations; there is no retroactive
exposure. Resuming a lane does not inherit a former parent's trace setting.

Trace ownership is independent of persistent/notify/auto-close policy. Persistent
children can be traced without tying their lifetime to the parent. Only the
validated parent can configure tracing; group membership or a claimed session ID
is not authority. Keep this association in memory, never in the durable row.

`events` exposes routing/receipt and Run/lane lifecycle metadata. `content` also
exposes Sessionbus message bodies. Native prompt/result bodies are outside the
initial scope. Trace scope is direct child traffic, not recursive tracing of
descendants or trace notifications.

## Live delivery only

Use a capability-negotiated `trace.event` notification and dedicated SDK handler.
Do not send copies through ordinary `message.send`, Deliver or Run: tracing must
not wake an idle-run parent, masquerade as a child message, recursively trace
itself or cause automatic inference. A wrapper may render events or hand them
to an existing consumer; it must not create a new trace storage subsystem.
Model-visible forwarding would be a separate explicit parent-selected policy,
with bounded volume.

The stream is best-effort. A full queue, timeout, unavailable parent or lost
connection drops trace events without delaying or changing ordinary traffic.
Do not resend the original message or retain trace events for reconnect. Report
loss counts on a subsequent live notification when possible. The end of a
connection or a new daemon incarnation is itself an observation discontinuity;
a crash can lose the loss counter too. Silence never proves no communication.

An event may carry a daemon incarnation and in-memory sequence to identify order
and gaps within that live stream. These are not history cursors and are never
persisted for resumption. No global ordering is inferred from host clocks.

## Explicit identities and truthful observations

Events include explicit `from` and `to`, canonical session IDs with `@host`,
names/products when authoritative, requested selectors, resolved recipients,
message_id, delivery_id, and run_id when known. Unresolved selectors remain
unresolved; no identity is invented. Do not serialize credentials, authorization
tokens, environment, arbitrary peer info, arguments or raw protocol frames.

Observe sends, recipient dispatches/rejections, receipts, Run boundaries and
lane lifecycle. A receipt remains the product's report or the daemon's explicit
loss classification. `written` does not prove consumption; `queued_for_next_turn`
does not identify a future consuming Run; `no_receipt` remains uncertain. A trace
cannot reconstruct consumption that an adapter never witnessed.

Projection follows the parent's policy at observation time. Parent `events` mode
never receives message content, even when operator content logging is enabled.
Increasing the mode later does not upgrade older queued events. On disable,
discard pending notifications for that child; do not recall events already sent.
With mixed child policies, expose only the authorized participant details.

## One copy when two traced children talk

For a common parent, one logical send produces at most one content-bearing trace
event identified by message_id. `matched_children` is the union of traced source
and recipient children. Matching both endpoints does not create a second copy.
Dispatch, late participant and receipt updates reference message_id/delivery_id
and never repeat the body. Group fan-out adds recipient metadata, not body copies.
Two deliberate sends with identical text have different IDs and stay distinct.

This holds across hosts too. Source/destination operator logs may each retain
physical observations, but those files do not feed the parent stream. Parent
trace emission needs one canonical emission decision per parent and logical
message, using authenticated routing and bounded in-flight state. Do not turn
an evicted deduplication entry into permission to emit a late second body; an
observation that cannot be correlated within the bound must be dropped with
loss reported when possible. No durable deduplication table is permitted.

Before implementation, specify the local/federated emission authority and its
lifetime/late-arrival rules. Test overlapping traced siblings, multicast,
interleaved receipts, saturation and connection replacement. A persistent index
or unbounded seen-ID set is not an acceptable way to satisfy deduplication.

## Federation and compatibility

Remote tracing controls/events follow authenticated federation routing and the
live trace-owner relationship. Link loss may lose events; it must not establish
fresh authority from a claimed owner_session_id. No remote trace query, backlog
transfer or recovery protocol is introduced. Unsupported remote hosts report
unsupported tracing rather than pretending to provide an empty complete stream.

The current hello and request schemas are closed. Plan an explicit protocol-2
capability boundary: new daemons continue protocol-1 service, while trace-aware
attachments negotiate protocol 2. Old daemons reject an explicit protocol-2
hello with generic invalid_hello and close. Clients explain the negotiation
failure, including the possibility of an older daemon, without silently falling
back to ordinary message copies.

The coordinated change covers both SDKs, federation validation, tool declarations
and drift tests, skills and product handling. Protocol 2 should carry delivery_id
in message.deliver so recipient and trace observations correlate by ID. No
resumable ownership capability or persisted subscription machinery is in scope.

## Operator logging: first implementation slice

The first slice retains content only for `message.send`. Run inputs and result
bodies are omitted; their IDs and lifecycle observations remain metadata. Every
message observation retains explicit `from` and `to`: canonical session IDs
with host qualification and names/products when authoritative. Requested
selectors stay distinct from resolved recipients; unresolved targets never get
fabricated session identities.

Operator service configuration:

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

## Acceptance requirements

- Trace-only operation performs no persistence writes in daemon, hub, SDK or
  wrapper. Run with operator logging disabled and trace-storage paths unavailable;
  tracing must not require or touch them. Ordinary pre-existing lane persistence
  is unchanged and contains no new tracing state.
- Restart/reconnect has no trace replay, restored backlog or historical access.
  Closing/forgetting a child creates no trace-history retention obligation.
- Default-off emits nothing to the parent. Only the validated parent configures
  a child, including running and persistent children.
- Events-only never exposes content; policy changes and queued-event handling
  cannot disclose previously unauthorized data.
- A common parent's two traced children produce one logical body copy, locally
  and across hosts. Duplicate text in distinct sends is not collapsed.
- Bound memory, counts and queued lifetimes under saturation, stalled peers,
  late federated arrivals and shutdown. Drop trace work rather than spill to disk
  or block ordinary routing; join all trace work on shutdown.
- Trace notifications do not wake Runs or recursively trace themselves. Missing
  events remain explicit uncertainty; no complete-audit claim is made.
- Mixed versions reject unsupported tracing through the capability boundary.

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
