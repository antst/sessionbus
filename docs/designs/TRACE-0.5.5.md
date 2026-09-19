# Trace corrections for bus 0.5.5

Status: bounded design for issues #80 and #81, 2026-09-19.
Base: `06782b9c1eecc5ca2cc951d90581f176865ea632`.
Both issues are required in 0.5.5 by the owner. No release/tag is authorized here.
The live-parent authority, ordinary delivery, projection bounds and no-new-
persistence contract in COMMUNICATION-TRACE.md remain in force.

## #80: report the effective mode to the spawning parent

Successful fresh and resumed `lane.spawn` responses include
`policy.trace: "off" | "events" | "content"`. Omitted spawn trace means `off`,
including resume; do not inherit the former live parent's mode. Derive the value
from the committed entry's live trace state under the directory lock, after
publication. Do not simply echo the requested value before publication.

Only a response addressed to the same still-live parent ownership may contain
this field. An ended/replaced parent or an unrelated observer does not receive
it. Persistent lifetime does not confer tracing authority. Existing ownership
checks and authenticated federation routing remain authoritative; session ID
text or group membership alone is insufficient.

Add an optional enum field to the protocol LanePolicy response DTO/schema and
JS type. It is optional to accept responses from older daemons; absence means
unreported, not proof of off. New successful parent responses report explicit
off as well as enabled modes. Build the response from a copy of the policy and
set trace only on that copy. Never set it on `entry.row.Policy`, normalized
stored policy, worker-open policy, or durable table rows. Keep existing list and
operator-roster projections unchanged in this slice; #80 describes a list echo
as desirable, while its requested fix is spawn/resume effective policy.

Implementation sites: daemon/lane.go currently returns a cloned row policy;
daemon/directory.go publishes live trace state; protocol/types.go and
protocol/session.schema.json define LanePolicy; sdk/js/protocol.d.ts mirrors it.
Add shared valid/invalid fixtures and Go/JS codec tests. Test fresh/resume for
all modes, omitted/default-off behavior, persistent children, live ownership
ending/replacement, and unchanged on-disk rows. Test remote spawning through
updated federation peers and that worker/list projections do not acquire trace.

### Compatibility is a real decoder constraint

Go sdk/internal/rpc/rpc.go:receiveResponse calls protocol.UnmarshalResult (or
DecodeResult for draining). The LanePolicy schema is closed, so the old SDK
rejects a response containing trace and closes that connection. JS
sdk/js/connection.js similarly validates the result with the closed schema.
This is not an automatically backward-compatible JSON addition.

Update the shared protocol schema, Go DTO, JS DTO and their fixtures together.
Build the bus, forwarding daemons/hubs and client SDK consumers from compatible
schema revisions before exercising the new response. The peers Go module pins
an older SDK, and its product bridges call Caller.Action through that SDK;
update that dependency and prove the spawn result survives the actual action
bridge. Audit the existing OpenCode/Kilo kit and DSH kit pins too before claiming
those consumers are compatible. An old response remains accepted by new clients;
old-client/new-response rejection must be explicitly demonstrated, not hidden by
loosening arbitrary unknown-field validation.

The public tool's spawn *input* already accepts trace. This correction introduces
no input option, so rewriting every product tool declaration is not presumed
necessary. Inspect actual result validation/declaration paths and change only
consumers that need the response field or SDK update. Coordinate the already
planned peers release/update as needed; do not invent a new version pin or tag
permission. Source compatibility and installed rollout are distinct from the
owner's separate authorization to publish releases.

## #81: omit a redundant copy to the sole original recipient

At the originating daemon's settled-send trace emission gate, for each eligible
parent, suppress its copy when the normal aggregate contains exactly one
resolved delivery and its canonical session ID is that parent's ID. Normal
routing already deduplicates aliases to one local recipient. Use the complete
aggregate before any per-parent projection, not the original selector text,
message body, target name, or per-child subset.

This applies to ordinary child-to-parent messages and daemon-generated lane
completion pointers equally. Do not blanket-exclude all daemon messages or
completion pointers: if the original went elsewhere, tracing remains useful.
Do not suppress another eligible parent's copy. If there are additional
recipients, an unresolved/rejected selector without a canonical recipient,
missing aggregate evidence, or no deliveries, keep existing trace behavior.
Do not claim an unresolved target is the parent merely because its spelling
matches. A canonical admitted recipient with an uncertain/rejected settled
receipt remains the same recipient; do not change receipt semantics.

The check belongs in daemon/trace.go:finishTrace, after live policy validation
and parent deduplication, before envelope creation/queue admission. It does not
modify the original send/result, block delivery, change the existing no-copies-
of-copies marker, create a retry, count intentional omission as queue loss, or
retain any state beyond the existing operation. All reservations still release.
No public DTO/schema change is required for #81.

Tests must cover local and federated child-to-parent messages, actual completion
pointers, both events/content, name/ID alias deduplication, another-recipient and
parent-plus-other fanout, missing/unresolved targets, different tracing parents,
and existing trace-copy loop prevention. Assert original receipt delivery and
bounded trace work cleanup. Retain tests of meaningful copies to other parents.
Update README and COMMUNICATION-TRACE.md with the sole-recipient exception.

## Review and validation

Keep the two corrections in separately reviewable commits. Source/fake-runtime
regression tests precede any installed test. No real product/model run is needed
to establish the daemon routing/SDK regressions; use the existing daemon and
federation fixtures and actual SDK transports. Installed final integration uses
the real permanent UMKA installation under the coordinated writer, with SDK-
compatible parents, no alternate installation or tracing persistence. No tag
on the current main; both fixes and their compatibility work must be complete
before 0.5.5 can be considered ready for the owner's publication decision.
