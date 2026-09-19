# Communication logging and parent-owned child tracing

Status: design history, 2026-09-14. Parent tracing shipped in Sessionbus v0.5.4
with the tool surface in sessionbus-peers v0.5.1. The
[README](../../README.md#parent-controlled-child-tracing) and
[usage guide](../USAGE.md#hosts-and-observation) describe the released contract
and its limits; this note records the design and its acceptance requirements.
Operator logging shipped separately in PR #71.
This design supersedes sibling isolation as the subject of issue #63;
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
  notifications, per-message parent-recipient selection and loss counters. Bound both bytes
  and event counts; queued work also needs a lifetime bound.
- Trace state is discarded when its live ownership ends or the process restarts.
  A reconnected parent may explicitly configure a new live subscription after
  normal ownership validation; there is no historical access to restore.
- The independently enabled operator JSONL log remains a diagnostic output.
  Tracing must work with it disabled and must never read it, index it, use it as
  a queue, or reconstruct trace state from it. This requirement authorizes no
  additional persistence in that logger either.

Ordinary delivery may already write the received message into the parent's native
transcript, just as any other message does. That existing product behavior is
unchanged; this requirement forbids adding trace-specific storage or recovery,
not the normal effects of delivering a message to the parent.

A design that requires durable state must be rejected or simplified. Relaxing
this constraint requires a separate explicit owner decision.

## Parent controls and scope

Child tracing defaults to **off**, is selected by the parent, and requires no
child approval. Operator logging and parent tracing are independent controls:
neither enables the other. Traces describe Sessionbus traffic, not arbitrary
shell/network activity, native prompts outside Sessionbus, or model reasoning.

The initial setting at spawn/resume is `trace: off | events | content`.
`trace.configure {session_id, mode}` changes a direct child's live policy, including
while it runs. Configuration returns the canonical child ID and effective mode. Enabling applies to subsequent observations; there is no retroactive
exposure. Resuming a lane does not inherit a former parent's trace setting.

Trace ownership is independent of persistent/notify/auto-close policy. Persistent
children can be traced without tying their lifetime to the parent. Only the
validated parent can configure tracing; group membership or a claimed session ID
is not authority. Keep this association in memory, never in the durable row.

`events` exposes message routing and settled delivery metadata. `content` also
exposes Sessionbus message bodies. Run/lane lifecycle events and native
prompt/result bodies are outside the initial scope. Trace scope is direct child
traffic, not recursive tracing of descendants or trace copies.

## Ordinary delivery, live copies only

A parent copy is a daemon-generated message sent through the existing message
routing, delivery queue, federation and receipt paths. Do not introduce a
`trace.event` transport, separate SDK event handler or trace delivery subsystem.
The parent receives the copy according to its ordinary delivery policy. In
particular, an idle-run parent may start a Run; a staging parent may receive a
queued-for-next-turn receipt. Enabling tracing opts into that existing behavior.
This supersedes the earlier proposal that tracing could never wake a parent.

Give the copy its own message ID and an explicit daemon-generated trace marker,
with the original message ID and original source/targets in the trace envelope.
Do not impersonate the original sender. The marker is daemon-owned routing
metadata, preserved across federation; text in a public message cannot set it.
Marked copies bypass parent-copy generation at every hop, including their own
delivery results. Their normal routing and receipt handling still apply.

0.5.5 correction (#81): omit a parent's redundant copy when the complete settled
original-send aggregate has exactly one resolved recipient and that canonical
session ID is the parent. This includes ordinary lane completion pointers sent
only to their tracing parent. Pointers sent elsewhere and fanout still produce
copies; unresolved selectors do not establish recipient identity. Check before
per-parent projection and do not count an intentional omission as queue loss.

Copy submission is best-effort. A full queue, timeout, unavailable parent or lost
connection drops trace events without delaying or changing ordinary traffic.
Do not resend the original message or retain trace copies for reconnect. The end of a
connection or a new daemon incarnation is itself an observation discontinuity;
a crash can lose the loss counter too. Silence never proves no communication.

Use existing message and delivery IDs for correlation. No trace sequence,
history cursor or global ordering mechanism is needed.

## Explicit identities and truthful observations

Events include explicit `from` and `to`, canonical session IDs with `@host`,
names/products when authoritative, requested selectors, resolved recipients,
message_id, delivery_id, and run_id when known. Unresolved selectors remain
unresolved; no identity is invented. Do not serialize credentials, authorization
tokens, environment, arbitrary peer info, arguments or raw protocol frames.

Parent tracing observes message sends and settled delivery results. The separate
operator logger also observes Run and lane lifecycle. A receipt remains the product's report or the daemon's explicit
loss classification. `written` does not prove consumption; `queued_for_next_turn`
does not identify a future consuming Run; `no_receipt` remains uncertain. A trace
cannot reconstruct consumption that an adapter never witnessed.

Projection follows the parent's policy at observation time. Parent `events` mode
never receives message content, even when operator content logging is enabled.
Increasing the mode later does not upgrade older queued events. `off` prevents subsequent admissions; it does not recall original sends already
admitted with a trace snapshot, so a copy may still arrive after `off`, including
from remote origins. Local child snapshots are discarded on any policy change
before emission. A copy is refused at arrival if its parent lifetime has ended.
With mixed child policies, expose only the authorized participant details.

## Copy at the delivery gate

Emit parent copies from the daemon's existing message-routing/delivery gate.
The source and resolved target are attributes of one routed message. They are
not separate trace records to register and match later. Checking their tracing
policies only selects recipients of the same copy.

For the initial implementation, emit the copy in the originating daemon's
existing delivery-result handler, when that logical send settles. The recipient
side produces its dispatch/receipt result; local routing or the hub's existing
return path already brings that result back to the originating operation. No
second observer or trace-specific result-matching process is needed.

For each logical message:

1. Capture the source and requested/resolved targets through ordinary routing.
2. Route the original message normally. Check the parent-selected policy of
   the source and each resolved target at the relevant routing boundary.
3. Use the existing send operation's delivery results, including rejections,
   failures or uncertain outcomes. Do not add a trace-specific wait or timeout.
4. Form the set of eligible parent recipients. A parent present through both
   source and target is present once; select its permitted projection.
5. Enqueue one trace copy per parent, containing the original source/target,
   permitted body and the relevant per-recipient status results. Use the parent's
   ordinary delivery queue and mark the copy as a daemon-generated trace message.
   Enqueue it without awaiting its receipt in the original send's completion path.

Placement determines what can be claimed. Sender ingress can report only a send
attempt. A recipient gate that queued a dispatch can report dispatch admission,
not native consumption. A returned peer receipt can report written, injected,
queued_for_next_turn or rejected according to the actual product contract.
Missing confirmation stays uncertain. Choosing the existing result handler gives
one copy with the best status already available to the normal send operation;
it deliberately does not provide an immediate pre-delivery copy as well.

The send result is not a promise about subsequent native consumption. There is
no retained job waiting to upgrade it later. No conversation reconstruction,
second source/target trace record or independent matching engine is needed.

For A -> B where both children have tracing enabled for parent P, the recipient
set is simply {P}; the gate creates one copy. For different parents P and Q,
the set is {P,Q}, each with its permitted projection. Group fan-out uses the same
set across that message's recipient loop, so siblings do not multiply body
copies. Equal text in separate sends remains separate traffic.

Keep the selected parent set only within the existing in-flight message
routing context. It is bounded transient routing bookkeeping, not a global
seen-message registry or retained trace history. A parent gets one copy of the settled send, with its permitted recipient results.
Do not add a second content event for each receipt. Drop work
that exceeds bounds rather than extending its lifetime or spilling to disk.

### Existing implementation boundaries

`session.send` already owns the original message ID and local recipient results;
`consumeReply` settles the pending local deliveries. Federated sends already use
`collectMessageSend` to return an aggregate through `finishRequest`. Attach the
copy decision to the originating operation's terminal result, covering immediate
rejections as well as asynchronous completion. Do not independently emit from
both a local leg and the aggregate, or from both `finishRequest` and its eventual
response writer. Retain only the original message projection and selected parent
recipients in the existing bounded operation lifetime.

Submit each copy as bounded daemon-owned ordinary message work. Its completion
releases that work through the existing delivery lifecycle; it neither changes
the original response nor invokes parent-copy generation. Shutdown must join it.
The implementation allows at most 256 trace operations and 4 MiB of retained
trace projections per daemon. A copy's ordinary delivery wait is bounded to five
seconds; expiration means confirmation was not obtained, not proof that the
copy was never delivered. There is no retry. These bounds never extend the
original send's wait.
The trusted trace marker and remote eligibility schema are internal federation
fields, not public recipient-selection parameters.

## One emitting daemon across federation

One logical message can cross several physical delivery gates. Therefore the
originating daemon owns the parent-copy decision for that message; hubs and
remote destination daemons must not independently emit second copies of it.
A daemon-generated original message also gets one originating routing context.

Each destination daemon knows its local target's live parent/trace policy.
Return the eligible parent routing information and target observation to the
originating daemon as typed metadata on the existing forwarding exchange.
The originating daemon merges those recipients into the same per-message set.
This is remote routing information, not a second trace stream to match. The hub
transports the exchange; it does not create a new copy at each hop. Operator
logs remain independent per-host diagnostic observations.

Remote eligibility and status ride the ordinary forwarding result; they do not
form a separate trace-report stream. Once the logical send settles, the origin
has the existing aggregate result from which to form each parent's one copy.
If the routing context ends or the reporting path is lost, retain only the
failure/uncertainty that normal routing actually established. Do not wait longer
for tracing, recreate the operation, replay a copy, or query logs.

The internal forwarding metadata carries a bounded set of child IDs, policy
modes/revisions, selected target labels and existing live parent routing references. Only the
authenticated daemon responsible for an endpoint can supply its policy; public
senders cannot inject trace recipients or grant access. Parent route information
must not become authority merely by carrying a claimed session ID. No durable
index or independent cross-host trace matcher is introduced.

## Federation and compatibility

Remote tracing controls/events follow authenticated federation routing and the
live trace-owner relationship. Link loss may lose events; it must not establish
fresh authority from a claimed owner_session_id. No remote trace query, backlog
transfer or recovery protocol is introduced. Explicit tracing controls and copies
require the negotiated `sessionbus-trace/1` federation capability. Updated hubs
report `unsupported_trace` when an involved link lacks it. This includes any
explicit spawn `trace` field; omitting that field retains the old spawn shape.
Ordinary messages still reach older hosts: optional eligibility collection is
disabled on that leg. No complete-stream guarantee is made. If adding internal
eligibility metadata would exceed the existing frame bound, omit that metadata
and retain the original ordinary result instead of failing the original send.

The current request schemas are closed. This change keeps protocol 1 and adds
the closed `trace.configure` method plus the optional closed `lane.spawn.trace`
field. Deploy the daemon, SDK validators and tool declarations together. An old
daemon rejects the new method or spawn field through its existing closed
decoder; clients must not treat that failure as an accepted trace policy or
silently fall back to an unmarked copy.

The coordinated change covers both SDKs, federation validation, tool declarations
and drift tests, skills and product handling. Reuse ordinary message delivery in
both SDKs; only trace controls and trusted daemon attribution need new handling.
No delivery field, uncertain-disposition rule, hello version, resumable ownership
capability or persisted subscription machinery changes in this slice.

## Operator logging: first implementation slice

Logging uses direct observations at existing handlers, not a message-matching or
deduplication engine. Each observing daemon writes the original message body
once at send admission. Dispatch and receipt records contain IDs and metadata,
not another body. Internal forwarding legs suppress the duplicate send header.
A parent copy is a separate marked message and is logged once as such, with its
own ID and original-message reference; its receipt does not generate a trace.

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

- Tracing adds no persistence writes or storage in daemon, hub, SDK or wrapper.
  Run with operator logging disabled and trace-storage paths unavailable;
  tracing must not require or touch them. Ordinary pre-existing lane persistence
  and native transcript behavior are unchanged and contain no new trace recovery
  state.
- Restart/reconnect has no trace replay, restored backlog or historical access.
  Closing/forgetting a child creates no trace-history retention obligation.
- Default-off emits nothing to the parent. Only the validated parent configures
  a child, including running and persistent children.
- Events-only never exposes content; policy changes and queued-event handling
  cannot disclose previously unauthorized data.
- A common parent's two traced children produce one logical body copy, locally
  and across hosts, through one emitter and a per-message parent-recipient set.
  Hubs and destination daemons cannot independently duplicate it. Distinct sends
  with equal text remain distinct; no global matching/seen-ID registry exists.
- Bound memory, counts and queued lifetimes under saturation, stalled peers,
  late federated arrivals and shutdown. Drop trace work rather than spill to disk
  or block ordinary routing; join all trace work on shutdown.
- Parent copies follow ordinary delivery policy, including idle-run or staging
  behavior. Neither copies nor their receipts recursively generate copies, even
  when the parent itself is a traced child or the copy crosses hosts.
- Original-send completion never waits for parent-copy delivery. A full or lost
  parent route cannot alter the original result or cause a retry.
- Each daemon logs a body once per message, with body-free dispatch/receipt rows;
  the separate marked parent copy has its own ID. No logging matcher is added.
- Missing copies remain explicit uncertainty; no complete-audit claim is made.
- Mixed versions reject unsupported tracing through the capability boundary.

## Deferred protocol-2 improvement: uncertain delivery

This section is not part of the initial parent-tracing implementation.

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
