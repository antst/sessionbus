// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestCallerMapsWireMethods(t *testing.T) {
	var seen []struct {
		method string
		params any
	}
	call := func(_ context.Context, method string, params, result any) error {
		seen = append(seen, struct {
			method string
			params any
		}{method, params})
		results := map[string]any{
			"session.list":   SessionListResult{Sessions: []SessionSummary{}},
			"message.send":   MessageSendResult{MessageID: "message", Deliveries: []protocol.MessageSendDelivery{}},
			"lane.describe":  LaneDescribeResult{Product: "example-peer", SupportedOpenFields: []string{}, ExtraArguments: []ExtraArgument{}},
			"lane.spawn":     LaneSpawnResult{SessionID: "lane@local"},
			"turn.run":       RunStatus{SessionID: "lane@local", RunID: "g/1", State: "done", Result: &TurnResult{Outcome: "completed", Result: "done"}},
			"turn.interrupt": struct{}{},
			"session.close":  struct{}{},
		}
		raw, _ := json.Marshal(results[method])
		return json.Unmarshal(raw, result)
	}
	c := newCaller(call)
	ctx := context.Background()
	table := []struct {
		method string
		params any
		call   func() error
	}{
		{"session.list", SessionListRequest{}, func() error { _, err := c.List(ctx, SessionListRequest{}); return err }},
		{"message.send", MessageSendRequest{Target: "lane", Message: "hello"}, func() error { _, err := c.Send(ctx, MessageSendRequest{Target: "lane", Message: "hello"}); return err }},
		{"lane.describe", LaneDescribeRequest{Product: "example-peer"}, func() error { _, err := c.Describe(ctx, LaneDescribeRequest{Product: "example-peer"}); return err }},
		{"lane.spawn", LaneSpawnRequest{Name: "child", Product: "example-peer", Open: &OpenOptions{}}, func() error {
			_, err := c.Spawn(ctx, LaneSpawnRequest{Name: "child", Product: "example-peer", Open: &OpenOptions{}})
			return err
		}},
		{"lane.spawn", LaneSpawnRequest{ResumeSessionID: "lane@local"}, func() error { _, err := c.Resume(ctx, "lane@local"); return err }},
		{"turn.run", TurnRunRequest{SessionID: "lane@local", Input: "work"}, func() error { _, err := c.Run(ctx, TurnRunRequest{SessionID: "lane@local", Input: "work"}); return err }},
		{"turn.interrupt", SessionTarget{SessionID: "lane@local"}, func() error { return c.Interrupt(ctx, SessionTarget{SessionID: "lane@local"}) }},
		{"session.close", SessionCloseRequest{SessionID: "lane@local", Forget: true}, func() error { return c.Close(ctx, SessionCloseRequest{SessionID: "lane@local", Forget: true}) }},
	}
	for index, test := range table {
		if err := test.call(); err != nil || seen[index].method != test.method || !reflect.DeepEqual(seen[index].params, test.params) {
			t.Fatalf("call %d: got %#v, err %v", index, seen[index], err)
		}
	}
}

func TestCallerSchemaErrorNamesPathAndConstraint(t *testing.T) {
	called := false
	caller := NewCaller(func(context.Context, string, any) (json.RawMessage, error) {
		called = true
		return nil, nil
	})
	_, err := caller.Spawn(context.Background(), LaneSpawnRequest{Name: "child", ResumeSessionID: "lane@local"})
	want := `LaneSpawnRequest: "name" is not allowed with "resume_session_id"`
	if err == nil || err.Error() != want || called {
		t.Fatalf("error = %v, called = %t", err, called)
	}
}

func TestDialIsOneShotFramedClient(t *testing.T) {
	root := testsocket.Directory(t)
	t.Setenv("XDG_RUNTIME_DIR", root)
	t.Setenv("SESSIONBUS_SOCKET", "")
	path := filepath.Join(root, "sessionbus", "presence.sock")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if got := Socket(); got != path {
		t.Fatalf("socket = %q, want %q", got, path)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		fd, _ := listener.Accept()
		requests := make(chan *rpc.Request, 1)
		server := rpc.New(fd, false, func(_ context.Context, request *rpc.Request) { requests <- request })
		_ = server.Result(<-requests, SessionListResult{Sessions: []SessionSummary{}})
	}()
	client, err := Dial("")
	if err != nil {
		t.Fatal(err)
	}
	caller := NewCaller(client.Call)
	listed, err := caller.List(context.Background(), SessionListRequest{})
	if err != nil || len(listed.Sessions) != 0 {
		t.Fatalf("result = %#v, err %v", listed, err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertJSON(t *testing.T, value, want any) {
	t.Helper()
	raw, err := json.Marshal(value)
	expected, wantErr := json.Marshal(want)
	if err != nil || wantErr != nil || string(raw) != string(expected) {
		t.Fatalf("json = %s, want %s, errors %v / %v", raw, expected, err, wantErr)
	}
}

func isCode(err error, code int) bool {
	var value *ProtocolError
	return errors.As(err, &value) && value.Code == code
}

type callerFixtures struct {
	Sequences struct {
		CrossedRehello crossedRehelloFixture `json:"crossed_rehello"`
	}
}

func loadCallerFixtures(t *testing.T) callerFixtures {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("protocol", "caller-sugar.fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures callerFixtures
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func TestCallerCancelledWaitRetainsWorkerResult(t *testing.T) {
	for _, action := range []bool{false, true} {
		for _, bounded := range []bool{false, true} {
			for _, collect := range []string{"status", "wait"} {
				t.Run(fmt.Sprintf("action=%t/bounded=%t/collect=%s", action, bounded, collect), func(t *testing.T) {
					release, entered := make(chan struct{}), make(chan struct{})
					requests := make(chan *rpc.Request, 16)
					p := &cursorProduct{run: func(context.Context, *Run, RunInput) (TurnResult, error) {
						close(entered)
						<-release
						return TurnResult{Outcome: "completed", Result: "retained"}, nil
					}}
					_, wire := cursorWire(t, p, func(r *rpc.Request) { requests <- r })
					cursorOpen(t, wire)
					if err := cursorExecute(wire, "native@local", "g/1"); err != nil {
						t.Fatal(err)
					}
					<-entered
					caller := newCaller(wire.Call)
					request := WaitRequest{SessionID: "native@local", RunID: "g/1"}
					if bounded {
						timeout := int64(60000)
						request.TimeoutMS = &timeout
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					waiting := make(chan error, 1)
					go func() {
						if action {
							raw, _ := json.Marshal(request)
							_, err := caller.Action(ctx, "wait", raw)
							waiting <- err
						} else {
							_, err := caller.WaitContext(ctx, request)
							waiting <- err
						}
					}()
					for r := range requests {
						if r.Method == "turn.wait" {
							break
						}
					}
					cancel()
					if err := <-waiting; !errors.Is(err, context.Canceled) {
						t.Fatalf("cancelled wait: %v", err)
					}
					close(release)
					// A replacement Caller reaches the same resident cursor; the canceled
					// correlation drains its late response before this subsequent reply.
					replacement := newCaller(wire.Call)
					got, err := replacement.Wait(WaitRequest{SessionID: request.SessionID, RunID: request.RunID})
					if err != nil || got.Result == nil || got.Result.Result != "retained" {
						t.Fatalf("retained %#v %v", got, err)
					}
					if _, err = caller.WaitContext(ctx, request); !errors.Is(err, context.Canceled) {
						t.Fatalf("pre-cancel wait %v", err)
					}
					if collect == "status" {
						got, err = replacement.Status(context.Background(), ReadRequest{SessionID: request.SessionID, RunID: request.RunID})
					} else {
						got, err = replacement.Wait(request)
					}
					if err != nil || got.Result == nil || got.Result.Result != "retained" {
						t.Fatalf("second read %#v %v", got, err)
					}
					ref := RunRef{SessionID: request.SessionID, RunID: request.RunID}
					if err = replacement.Ack(ctx, ref); !errors.Is(err, context.Canceled) {
						t.Fatalf("pre-canceled ack %v", err)
					}
					if _, err = replacement.Status(context.Background(), ReadRequest{SessionID: ref.SessionID, RunID: ref.RunID}); err != nil {
						t.Fatal(err)
					}
					if err = replacement.Ack(context.Background(), ref); err != nil {
						t.Fatal(err)
					}
					_, err = replacement.Status(context.Background(), ReadRequest{SessionID: ref.SessionID, RunID: ref.RunID})
					cursorCode(t, err, protocol.UnknownSession)
				})
			}
		}
	}
}

func TestCallerSubmittedAckSettlesDespiteCancellation(t *testing.T) {
	client, server := net.Pipe()
	requests := make(chan *rpc.Request, 1)
	daemon := rpc.New(server, false, func(_ context.Context, r *rpc.Request) { requests <- r })
	wire := rpc.New(client, true, func(context.Context, *rpc.Request) {})
	t.Cleanup(func() { _ = wire.Close(); _ = daemon.Close() })
	caller := newCaller(wire.Call)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- caller.Ack(ctx, RunRef{SessionID: "native@local", RunID: "g/1"}) }()
	request := <-requests
	cancel()
	if err := daemon.Result(request, struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("submitted ack abandoned: %v", err)
	}
}
