// SPDX-License-Identifier: MIT

package sessionkit_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestConnectionDuplex(t *testing.T) {
	requests := make(chan *sessionkit.Request, 1)
	c, peer := connectionPair(t, func(_ context.Context, request *sessionkit.Request) { requests <- request })
	done := make(chan error, 1)
	var result protocol.SessionListResult
	go func() { done <- c.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &result) }()
	reader := bufio.NewReader(peer)
	call := connectionFrame(t, reader)
	for _, disposition := range []string{"written", "uncertain"} {
		id := int64(1)
		if disposition == "uncertain" {
			id = 2
		}
		body, err := protocol.RequestBytes(id, "message.deliver", protocol.DeliveryRequest{
			MessageID: "m", From: protocol.DeliverySource{SessionID: "sender@local", Product: "example-peer", Groups: []string{}}, Body: "hello",
		})
		connectionMust(t, err)
		_, err = peer.Write(body)
		connectionMust(t, err)
		request := <-requests
		params, ok := request.Params.(*protocol.DeliveryRequest)
		if !ok || params.Body != "hello" || params.From.Name != "" || request.ID != id || request.Method != "message.deliver" {
			t.Fatalf("request = %#v", request)
		}
		if disposition == "written" {
			go func() { done <- c.Result(request, protocol.DeliveryReceipt{Disposition: "written"}) }()
		} else {
			go func() { done <- c.Error(request, protocol.Internal, "transport lost after submission") }()
		}
		reply := connectionFrame(t, reader)
		if reply.ID != id || disposition == "written" && string(reply.Result) != `{"disposition":"written"}` || disposition == "uncertain" && (reply.Error == nil || reply.Error.Code != protocol.Internal) {
			t.Fatalf("reply = %#v", reply)
		}
		connectionMust(t, <-done)
	}
	body, err := protocol.ResultBytes(call.ID, "session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
	connectionMust(t, err)
	_, err = peer.Write(body)
	connectionMust(t, err)
	connectionMust(t, <-done)
	if result.Sessions == nil {
		t.Fatal("outbound call did not decode its result")
	}
}

func TestConnectionSharedFixtures(t *testing.T) {
	var fixtures struct {
		Cases []struct {
			Name, Definition string
			Valid            bool
			Value            json.RawMessage
		}
	}
	raw, err := os.ReadFile("protocol/session.fixtures.json")
	connectionMust(t, err)
	connectionMust(t, json.Unmarshal(raw, &fixtures))
	for _, fixture := range fixtures.Cases {
		switch fixture.Definition {
		case "SessionHelloResult":
			t.Run(fixture.Name, func(t *testing.T) {
				var observed atomic.Bool
				dispatched := make(chan bool, 1)
				c, peer := connectionPair(t, func(_ context.Context, _ *sessionkit.Request) { dispatched <- observed.Load() })
				done := make(chan error, 1)
				go func() {
					done <- c.CallObserved(context.Background(), "session.hello", protocol.PeerHello{
						Protocol: 1, Product: "example-peer", SessionID: "native", Groups: []string{}, Info: map[string]any{},
					}, &struct{}{}, func() error { observed.Store(true); return nil })
				}()
				call := connectionFrame(t, bufio.NewReader(peer))
				// One write puts the next request immediately after the acknowledgement.
				_, err := fmt.Fprintf(peer, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":%s}\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session.superseded\",\"params\":{}}\n", call.ID, fixture.Value)
				connectionMust(t, err)
				if err := <-done; (err == nil) != fixture.Valid {
					t.Fatalf("call error = %v, valid = %v", err, fixture.Valid)
				}
				if fixture.Valid {
					if !<-dispatched {
						t.Fatal("request dispatched before acknowledgement callback")
					}
				} else {
					<-c.Done()
					if observed.Load() || len(dispatched) != 0 {
						t.Fatal("invalid result observed or later request dispatched")
					}
				}
			})
		case "SessionSupersededRequest":
			t.Run(fixture.Name, func(t *testing.T) {
				dispatched := make(chan *sessionkit.Request, 1)
				c, peer := connectionPair(t, func(_ context.Context, request *sessionkit.Request) { dispatched <- request })
				_, err := fmt.Fprintf(peer, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session.superseded\",\"params\":%s}\n", fixture.Value)
				connectionMust(t, err)
				if fixture.Valid {
					if request := <-dispatched; request.Method != "session.superseded" {
						t.Fatalf("request = %#v", request)
					}
				} else {
					reply := connectionFrame(t, bufio.NewReader(peer))
					if reply.Error == nil || reply.Error.Code != protocol.InvalidFrame {
						t.Fatalf("reply = %#v", reply)
					}
					<-c.Done()
					if len(dispatched) != 0 {
						t.Fatal("invalid request dispatched")
					}
				}
			})
		}
	}
}

func TestConnectionEOFSettlesPendingCall(t *testing.T) {
	c, peer := connectionPair(t, func(context.Context, *sessionkit.Request) { t.Error("unexpected inbound request") })
	go func() { _, _ = bufio.NewReader(peer).ReadBytes('\n'); _ = peer.Close() }()
	done := c.Begin("session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{}, nil)
	if err := <-done; !errors.Is(err, io.EOF) {
		t.Fatalf("pending call error = %v", err)
	}
	<-c.Done()
	if !errors.Is(c.Context().Err(), context.Canceled) {
		t.Fatal("connection context not cancelled")
	}
	connectionMust(t, c.Close())
	connectionMust(t, c.Close())
}

func connectionPair(t *testing.T, handler func(context.Context, *sessionkit.Request)) (*sessionkit.Connection, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	connectionMust(t, peer.SetDeadline(time.Now().Add(5*time.Second)))
	c := sessionkit.NewConnection(local, handler)
	t.Cleanup(func() { _ = c.Close(); _ = peer.Close() })
	return c, peer
}

func connectionFrame(t *testing.T, reader *bufio.Reader) protocol.Frame {
	t.Helper()
	body, err := reader.ReadBytes('\n')
	connectionMust(t, err)
	frame, err := protocol.DecodeFrame(body[:len(body)-1])
	connectionMust(t, err)
	return frame
}

func connectionMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
