// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func reconnectSecrets() map[string]string {
	secrets := map[string]string{}
	for i, host := range []string{"alpha", "beta"} {
		secret := make([]byte, federation.SecretBytes)
		secret[0] = byte(i + 1)
		secrets[host] = base64.StdEncoding.EncodeToString(secret)
	}
	return secrets
}

func reconnectHub(t *testing.T, address string, secrets map[string]string) (*federation.Hub, string) {
	t.Helper()
	configuration, err := federation.ServerTLS(secrets)
	must(t, err)
	listener, err := net.Listen("tcp", address)
	must(t, err)
	hub := federation.StartHub(listener, configuration, io.Discard)
	t.Cleanup(hub.Close)
	return hub, listener.Addr().String()
}

func reconnectDaemon(t *testing.T, host, address, secret string) (*Daemon, string) {
	t.Helper()
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "daemon.sock")
	d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(directory, "rows"),
		Host: host, HubAddress: address, HubSecret: secret})
	must(t, err)
	t.Cleanup(func() { must(t, d.Close()) })
	return d, socket
}

func awaitRemotePeer(t *testing.T, peer *peerClient, host, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		raw, err := peer.peer.Call(ctx, "session.list", protocol.SessionListRequest{Host: host})
		cancel()
		lastErr = err
		var listed protocol.SessionListResult
		if err == nil && json.Unmarshal(raw, &listed) == nil {
			for _, row := range listed.Sessions {
				if row.SessionID == id && row.Connected {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("existing peer never saw %s after hub recovery: %v", id, lastErr)
}

func TestFederationReconnectsAfterHubRestartWithoutRestartingDaemons(t *testing.T) {
	secrets := reconnectSecrets()
	hub, address := reconnectHub(t, "127.0.0.1:0", secrets)
	alpha, alphaSocket := reconnectDaemon(t, "alpha", address, secrets["alpha"])
	beta, betaSocket := reconnectDaemon(t, "beta", address, secrets["beta"])
	sender := connectPeer(t, alphaSocket, "sender", "sender", "team")
	receiver := connectPeer(t, betaSocket, "receiver", "receiver", "team")
	awaitRemotePeer(t, sender, "beta", "receiver@beta")

	for attempt := 0; attempt < 2; attempt++ {
		hub.Close()
		for _, d := range []*Daemon{alpha, beta} {
			select {
			case <-d.Done():
				t.Fatal("hub loss stopped the local daemon")
			default:
			}
		}
		var local protocol.SessionListResult
		must(t, sender.call("session.list", protocol.SessionListRequest{Host: "alpha"}, &local))
		if local.SelfInfo == nil || local.SelfInfo.SessionID != "sender@alpha" {
			t.Fatalf("local caller changed during outage: %#v", local.SelfInfo)
		}
		hub, _ = reconnectHub(t, address, secrets)
		awaitRemotePeer(t, sender, "beta", "receiver@beta")
		var sent protocol.MessageSendResult
		must(t, sender.call("message.send", protocol.MessageSendRequest{Target: "receiver@beta", Message: "after restart"}, &sent))
		if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "injected" {
			t.Fatalf("restored federation delivery: %#v", sent)
		}
		select {
		case delivery := <-receiver.deliveries:
			if delivery.Body != "after restart" || delivery.From.SessionID != "sender@alpha" {
				t.Fatalf("restored delivery identity: %#v", delivery)
			}
		case <-time.After(time.Second):
			t.Fatal("restored delivery missing")
		}
	}
}

func TestFederationStartsLocallyWhileHubIsUnavailable(t *testing.T) {
	secrets := reconnectSecrets()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	address := reserved.Addr().String()
	must(t, reserved.Close())
	_, alphaSocket := reconnectDaemon(t, "alpha", address, secrets["alpha"])
	sender := connectPeer(t, alphaSocket, "sender", "sender", "team")
	var local protocol.SessionListResult
	must(t, sender.call("session.list", protocol.SessionListRequest{Host: "alpha"}, &local))
	_, _ = reconnectHub(t, address, secrets)
	_, betaSocket := reconnectDaemon(t, "beta", address, secrets["beta"])
	_ = connectPeer(t, betaSocket, "receiver", "receiver", "team")
	awaitRemotePeer(t, sender, "beta", "receiver@beta")
}

func TestFederationReconnectDoesNotReplayAdmittedDelivery(t *testing.T) {
	secrets := reconnectSecrets()
	hub, address := reconnectHub(t, "127.0.0.1:0", secrets)
	_, alphaSocket := reconnectDaemon(t, "alpha", address, secrets["alpha"])
	_, betaSocket := reconnectDaemon(t, "beta", address, secrets["beta"])
	sender := connectPeer(t, alphaSocket, "sender", "sender", "team")
	t.Setenv("SESSIONBUS_SOCKET", betaSocket)
	received := make(chan sessionkit.DeliveryRequest, 8)
	release := make(chan struct{})
	defer close(release)
	receiver, err := sessionkit.ConnectPeer(sessionkit.PeerIdentity{Product: "fixture-client", SessionID: "receiver", Name: "receiver", Groups: []string{"team"}, Info: map[string]any{}},
		func(ctx context.Context, _ sessionkit.PeerIdentity, request sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
			received <- request
			if request.Body == "held before outage" {
				select {
				case <-release:
				case <-ctx.Done():
					return sessionkit.DeliveryReceipt{}, ctx.Err()
				}
			}
			return sessionkit.DeliveryReceipt{Disposition: "injected"}, nil
		})
	must(t, err)
	t.Cleanup(func() { receiver.Shutdown(); <-receiver.Closed() })
	awaitRemotePeer(t, sender, "beta", "receiver@beta")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type callResult struct {
		raw json.RawMessage
		err error
	}
	calling := make(chan callResult, 1)
	go func() {
		raw, callErr := sender.peer.Call(ctx, "message.send", protocol.MessageSendRequest{Target: "receiver@beta", Message: "held before outage"})
		calling <- callResult{raw, callErr}
	}()
	select {
	case request := <-received:
		if request.Body != "held before outage" {
			t.Fatalf("unexpected admitted request: %#v", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not reach remote handler")
	}
	hub.Close()
	select {
	case result := <-calling:
		must(t, result.err)
		var sent protocol.MessageSendResult
		must(t, json.Unmarshal(result.raw, &sent))
		// message.send projects a lost forwarded receipt as no_receipt: the
		// sender cannot infer whether the admitted remote operation executed.
		if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "rejected" || sent.Deliveries[0].Reason != "no_receipt" {
			t.Fatalf("admitted request must report no_receipt: %+v", sent)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("old admitted request was not settled after hub loss")
	}
	_, _ = reconnectHub(t, address, secrets)
	awaitRemotePeer(t, sender, "beta", "receiver@beta")
	_, err = sender.peer.Call(ctx, "message.send", protocol.MessageSendRequest{Target: "receiver@beta", Message: "new explicit request"})
	must(t, err)
	select {
	case request := <-received:
		if request.Body != "new explicit request" {
			t.Fatalf("recovery replayed old request: %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("new explicit request was not delivered")
	}
	select {
	case request := <-received:
		t.Fatalf("extra delivery after recovery: %#v", request)
	default:
	}
}

func TestFederationCloseCancelsStalledTLSHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		fd, _ := listener.Accept()
		accepted <- fd
	}()
	d, _ := reconnectDaemon(t, "alpha", listener.Addr().String(), reconnectSecrets()["alpha"])
	var fd net.Conn
	select {
	case fd = <-accepted:
		if fd == nil {
			t.Fatal("accept failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not attempt a TLS connection")
	}
	t.Cleanup(func() { _ = fd.Close() })
	readDone := make(chan error, 1)
	go func() {
		_, readErr := io.Copy(io.Discard, fd)
		readDone <- readErr
	}()
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case err := <-closed:
		must(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("daemon Close did not cancel and join the TLS handshake")
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("TLS socket remained open after daemon Close")
	}
}

func TestFederationCloseJoinsUnavailableHubRetry(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	address := listener.Addr().String()
	must(t, listener.Close())
	d, socket := reconnectDaemon(t, "alpha", address, reconnectSecrets()["alpha"])
	_ = connectPeer(t, socket, "local", "local", "team")
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case err := <-closed:
		must(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("daemon Close did not join the unavailable-hub supervisor")
	}
}

func TestFederationRejectsMalformedAddressBeforeStartingSupervisor(t *testing.T) {
	for _, address := range []string{"127.0.0.1::7419", "localhost", "localhost:", "localhost:not-a-tcp-service"} {
		t.Run(address, func(t *testing.T) {
			directory := testsocket.Directory(t)
			d, err := Start(Config{SocketPath: filepath.Join(directory, "daemon.sock"), TablePath: filepath.Join(directory, "rows"),
				Host: "alpha", HubAddress: address, HubSecret: reconnectSecrets()["alpha"]})
			if err == nil {
				_ = d.Close()
				t.Fatal("malformed hub address was accepted")
			}
		})
	}
}
