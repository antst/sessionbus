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
configuration, keys, state and the other role are preserved. Product peers
install separately from [sessionbus-peers](https://github.com/antst/sessionbus-peers).

The default is the **development prerelease**, updated after green develop CI.
To pin a published version, set the variable on **sh**, not curl:

```sh
curl -fsSL https://raw.githubusercontent.com/antst/sessionbus/develop/deploy/install-host.sh | SESSIONBUS_VERSION=vX.Y.Z sh
```

Replace `vX.Y.Z` with an actual [release tag](https://github.com/antst/sessionbus/releases).
`SESSIONBUS_DOWNLOAD_ROOT` can select a mirror containing the same archives and
`SHA256SUMS`. Missing releases or checksum failures stop before installation.
You can download and inspect the script before executing it.

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
development artifacts after successful develop CI. Maintainers can dispatch it
with a new `vX.Y.Z` tag for an immutable stable release. `SOURCE.txt` records the
commit; archives contain the revision and license. Stable tags are never
overwritten. Manual release builds also run the repository tests.

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
