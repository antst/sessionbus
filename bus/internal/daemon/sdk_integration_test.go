// SPDX-License-Identifier: GPL-3.0-only

package daemon_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/daemon"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestSDKCallerReportsRealDaemonEOF(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "sessionbus.sock")
	service, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(filepath.Dir(socket), "sessions")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	caller := connectPeer(t, socket, "caller", func(context.Context, sessionkit.PeerIdentity, sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
		return sessionkit.DeliveryReceipt{Disposition: "injected"}, nil
	})
	received := make(chan struct{}, 1)
	connectPeer(t, socket, "lane", func(ctx context.Context, _ sessionkit.PeerIdentity, _ sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
		received <- struct{}{}
		<-ctx.Done()
		return sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "closing"}, nil
	})
	runs := sessionkit.NewCaller(func(ctx context.Context, _ string, _ any) (json.RawMessage, error) {
		return caller.Call(ctx, "message.send", sessionkit.MessageSendRequest{Target: "lane@local", Message: "hold"})
	})
	started, err := runs.Start(sessionkit.TurnRunRequest{SessionID: "lane@local", Input: "hold"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("run did not reach target")
	}
	if err = service.Close(); err != nil {
		t.Fatal(err)
	}
	status, err := runs.Wait(sessionkit.WaitRequest{TurnID: started.TurnID})
	if err != nil || status.State != "unavailable" || status.Reason != "result unavailable, lane resumable" {
		t.Fatalf("daemon EOF status = %#v, %v", status, err)
	}
}

func TestSDKPeerDaemonRehelloRules(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "sessionbus.sock")
	service, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(filepath.Dir(socket), "sessions")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	peer := connectPeer(t, socket, "peer", func(context.Context, sessionkit.PeerIdentity, sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
		return sessionkit.DeliveryReceipt{Disposition: "injected"}, nil
	})
	if err = peer.Rehello(context.Background(), "renamed", map[string]any{"revision": 2}); err != nil {
		t.Fatal(err)
	}
	listed, err := peer.Caller.List(context.Background(), sessionkit.SessionListRequest{})
	if err != nil || len(listed.Sessions) != 1 || listed.Sessions[0].Name != "renamed@local" {
		t.Fatalf("same-id rehello = %#v, %v", listed.Sessions, err)
	}
	client, err := sessionkit.Dial(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	identity := sessionkit.PeerIdentity{Protocol: 1, Product: "fixture-client", SessionID: "raw", Name: "raw", Groups: []string{"one", "two"}, Info: map[string]any{}}
	if _, err = client.Call(context.Background(), "session.hello", identity); err != nil {
		t.Fatal(err)
	}
	identity.Groups = []string{"two", "one"}
	_, err = client.Call(context.Background(), "session.hello", identity)
	var protocolError *sessionkit.ProtocolError
	if !errors.As(err, &protocolError) || protocolError.Code != protocol.InvalidHello {
		t.Fatalf("changed groups = %v", err)
	}
}

func connectPeer(t *testing.T, socket, id string, deliver sessionkit.DeliverFunc) *sessionkit.Peer {
	t.Helper()
	t.Setenv("SESSIONBUS_SOCKET", socket)
	peer, err := sessionkit.ConnectPeer(sessionkit.PeerIdentity{Product: "fixture-client", SessionID: id, Name: id, Groups: []string{"shared"}, Info: map[string]any{}}, deliver)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-peer.Ready():
	case <-time.After(time.Second):
		t.Fatal("peer ready timed out")
	}
	t.Cleanup(func() {
		peer.Shutdown()
		select {
		case <-peer.Closed():
		case <-time.After(time.Second):
			t.Fatal("peer close timed out")
		}
	})
	return peer
}
