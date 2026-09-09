// SPDX-License-Identifier: MIT

package rpc

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestCancelledCallsDrainAtBound(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	c := New(local, true, nil)
	defer c.Close()
	observed := false
	target := protocol.SessionListResult{Sessions: []protocol.SessionSummary{{SessionID: "unchanged"}}}
	for range protocol.MaxOperations {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- c.CallObserved(ctx, "session.list", protocol.SessionListRequest{}, &target, func() error { observed = true; return nil })
		}()
		readFrame(t, peer)
		cancel()
		if !errors.Is(<-done, context.Canceled) {
			t.Fatal("cancel did not win")
		}
	}
	err := c.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &target)
	var wire *protocol.RPCError
	if !errors.As(err, &wire) || wire.Code != protocol.Busy {
		t.Fatalf("bound: %v", err)
	}
	for id := int64(1); id <= protocol.MaxOperations; id++ {
		body, encodeErr := protocol.ResultBytes(id, "session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
		must(t, encodeErr)
		_, err = peer.Write(body)
		must(t, err)
	}
	// A further round trip establishes that every earlier response was drained.
	done := make(chan error, 1)
	go func() {
		done <- c.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{})
	}()
	request := readFrame(t, peer)
	body, _ := protocol.ResultBytes(request.ID, "session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
	_, err = peer.Write(body)
	must(t, err)
	must(t, <-done)
	if observed || len(target.Sessions) != 1 || target.Sessions[0].SessionID != "unchanged" {
		t.Fatal("abandoned target/observer was used")
	}
}

func TestCancelledReplyStillValidates(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	c := New(local, true, nil)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Call(ctx, "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{})
	}()
	readFrame(t, peer)
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("cancel did not win")
	}
	_, _ = peer.Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"))
	<-c.Done()
}
