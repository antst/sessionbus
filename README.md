# Sessionbus

Sessionbus is a local and federated session router. This repository contains
the `sessionbus` daemon, the `sessionbus-hub` federation router, the
`sessionbus-call` reference caller, and the `example-peer` protocol worker.

## Install binaries

Install the normal host (daemon, reference caller and example worker; no hub):

```sh
curl -fsSL https://raw.githubusercontent.com/antst/sessionbus/develop/deploy/install-host.sh | sh
```

Install only the federation hub:

```sh
curl -fsSL https://raw.githubusercontent.com/antst/sessionbus/develop/deploy/install-hub.sh | sh
```

Linux and macOS, amd64 and arm64 are supported. Run as your normal login user;
no sudo, Go, npm, or checkout is required. The scripts verify release SHA256
checksums and install under `~/.local`, using systemd user services on Linux or
launchd on macOS. Linux needs a working user service manager. Only the selected
role is restarted. Add `~/.local/bin` to your login PATH if prompted. Existing
configuration, keys, state and the other role are preserved. An earlier real
`current` directory is retained under `releases/prior.*/current` when migrating
to the release symlink. Product peers
install separately from [sessionbus-peers](https://github.com/antst/sessionbus-peers).

The default is GitHub's **latest stable release** (`SESSIONBUS_VERSION=latest`),
using its `/releases/latest/download/` endpoint. Prereleases are not selected.
To pin a published version, set the variable on **sh**, not curl:

```sh
curl -fsSL https://raw.githubusercontent.com/antst/sessionbus/develop/deploy/install-host.sh | SESSIONBUS_VERSION=vX.Y.Z sh
```

Replace `vX.Y.Z` with an actual [release tag](https://github.com/antst/sessionbus/releases).
Development builds are opt-in: use `SESSIONBUS_VERSION=development sh` in the
same pipeline. The rolling development prerelease follows tested `develop`
builds and can change between installations.
See the [v0.5.1 release notes](docs/releases/v0.5.1.md) for scope and upgrade order.
`SESSIONBUS_DOWNLOAD_ROOT` can select a mirror containing the same archives and
`SHA256SUMS`. Missing releases or checksum failures stop before installation.
You can download and inspect the script before executing it.

## See what is running

```sh
sessionbus roster
sessionbus roster --json
sessionbus roster --local
sessionbus roster --all
```

`roster` is the daemon owner's operational view of **all groups**. By default it
shows online peers and lanes, plus any lane with an active Run
(`connected || running`).
Use `--all` to include offline and retained/archived entries. This filter applies
to both table and JSON output, on local and remote hosts. Rows include product,
name, session ID, groups,
connected/running state, lane owner, persistence, and requested lane permission
mode. Federated hosts contribute the same live metadata. It does not expose
messages, results, native arguments, arbitrary peer `info`, credentials, or
ownership tokens, and it does not register an observer peer.

The operator socket is mode 0600 inside the current user's mode-0700 runtime
directory. This is same-user diagnostic access; ordinary `session.list` calls
remain group-restricted. Authenticated federation hosts are trusted to request
each other's operator metadata. `roster --socket PATH` selects the daemon using
its **public** socket path; the operator endpoint is derived automatically.

`--json` emits `sessionbus.roster.v1`. Unavailable remote metadata is marked
explicitly and the command exits nonzero for an incomplete roster; `--local`
skips federation. Upgrade the hub and hosts to v0.5.1 for complete federated
rosters. Older links continue working and are never sent unsupported roster
requests. No native permission mode is inferred for a peer that does not have
a daemon-owned lane Open record.

For caller-scoped protocol access, the reference caller remains available:

```sh
sessionbus-call -g YOUR_GROUP session.list '{}'
```

Use `sessionbus --help`, `sessionbus help roster`, `sessionbus help secret`,
`sessionbus-hub --help`, or `sessionbus-hub add-host --help` for the actual CLI
commands and flags. These help commands do not start services or alter keys.

### Connect hosts to a hub

Host installation generates `~/.config/sessionbus/host.key` once (mode 0600).
This is a **shared join secret**, not a public key. Reinstallation preserves it;
the installer never prints it. Transfer it securely to the hub administrator.
The hub starts with an empty mode-0600 `~/.config/sessionbus/hub.json` host map.
On the hub:

```sh
chmod 600 /path/to/copied-host.key
sessionbus-hub add-host -secret-file /path/to/copied-host.key workstation
systemctl --user restart sessionbus-hub
```

On the host, add or update these entries in `~/.config/sessionbus/service.env`,
preserving other settings:

```sh
SESSIONBUS_HOST=workstation
SESSIONBUS_HUB=hub.example:7419
SESSIONBUS_HUB_SECRET_FILE="/home/YOUR_USER/.config/sessionbus/host.key"
```

Use the actual absolute key path printed by the installer. Restart the host
with `systemctl --user restart sessionbus`. On macOS use
`launchctl kickstart -k gui/$(id -u)/net.antst.sessionbus` (append `-hub` for the
hub). Allow TCP 7419 through the hub firewall as appropriate. Host names must
match the registration. `add-host` is idempotent for the same name/key, rejects
implicit key replacement and duplicate secrets, and edits configuration only;
restart the hub to load it. `SESSIONBUS_HUB_LISTEN` in `hub.env` changes the
listener. `XDG_CONFIG_HOME` changes the installer and `add-host` default configuration location.
The host installer waits at most ten seconds for an authenticated local list response.
Hub installation reports service activation only; inspect the service status/logs
to confirm its listener. Preserved `hub.env` can override its default port and host map.

### Build or publish releases

`deploy/package-release OUTPUT_DIRECTORY` builds separate host/hub archives;
set `GOOS`/`GOARCH` to cross-compile. The `Binary releases` workflow publishes
development artifacts from `develop` pushes after its tests and builds pass. Maintainers publish an immutable stable release by pushing a new `vX.Y.Z`
tag pointing at reviewed source. `SOURCE.txt` records the
commit; archives contain the revision and license. Stable tags are never
overwritten. Tag-triggered release builds also run the repository tests.

## SDKs and development

The Go SDK is the independent module
`github.com/antst/sessionbus/bus/sdk/go`; the JavaScript SDK is published as
`@sessionbus/kit`. The daemon is GPL-3.0-only under the root [LICENSE](LICENSE).
The Go and JavaScript SDKs are MIT-licensed under
[`bus/sdk/go/LICENSE`](bus/sdk/go/LICENSE) and
[`bus/sdk/js/LICENSE`](bus/sdk/js/LICENSE).

Go connection owners can use `NewConnection(fd, handler)` with an already
connected `net.Conn`. The public `Connection` and `Request` aliases expose the
existing validated duplex RPC implementation, including `Call`, `CallObserved`,
`Begin`, `Result`, and `Error`. The owner controls registration and connection
lifetime; there is no automatic reconnect. The non-nil handler and
`CallObserved` callback run on the reader in frame order, so they must offload
blocking work. `CallObserved` observes a valid decoded result before the next
inbound frame is dispatched. `Close` closes the socket and cancels its context.

Caller waits are cancellable: JavaScript `caller.wait(request, signal)` and
`caller.action("wait", request, signal)` accept an `AbortSignal`; Go provides
`caller.WaitContext(ctx, request)` and forwards the context from `Action`.
Cancellation stops only that wait and preserves the run/result handle for a
later status or wait call. It does not interrupt the native run. Existing Go
`Wait(request)` remains available without cancellation.

List replies include `self_info` with the bound caller's canonical `session_id`,
optional `name`, `product`, and `groups`, even when filters select other sessions
or hosts. Compare `self_info.session_id` with row IDs to identify self. Go exposes
it as `SessionListResult.SelfInfo`; JavaScript preserves the wire `self_info`
object. Updated SDKs accept omission from older daemons (Go: `nil`), which means
the response does not identify self. Upgrade clients before the daemon: earlier
SDKs reject this new response field.

Run the repository gates with:

```sh
go test -race ./...
go vet ./...
(cd bus/sdk/go && GOWORK=off go test -race ./... && GOWORK=off go vet ./...)
npm test --prefix bus
```

Product peers live in [antst/sessionbus-peers](https://github.com/antst/sessionbus-peers).
The signed designs are in [`docs/designs`](docs/designs), and the generated
wire reference is [`bus/docs/PROTOCOL.md`](bus/docs/PROTOCOL.md). The longer
direction is described in [`docs/END-GOAL.md`](docs/END-GOAL.md).

Pre-split history remains on the `legacy-*` branches of Forgejo repository
`ai/sessionbus`.
