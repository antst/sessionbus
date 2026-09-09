# Sessionbus

Sessionbus is a local and federated session router. This repository contains
the `sessionbus` daemon, the `sessionbus-hub` federation router, the
`sessionbus-call` reference caller, and the `example-peer` protocol worker.

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
