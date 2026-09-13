// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type cursorProduct struct {
	close func()
	open  func()
	run   func(context.Context, *Run, RunInput) (TurnResult, error)
	calls atomic.Int32
}

func (*cursorProduct) Hello(context.Context) (HelloDescription, error) {
	return HelloDescription{}, nil
}
func (p *cursorProduct) Open(context.Context, OpenRequest) (OpenResult, error) {
	if p.open != nil {
		p.open()
	}
	return OpenResult{SessionID: "native"}, nil
}
func (p *cursorProduct) Run(ctx context.Context, run *Run, input RunInput) (TurnResult, error) {
	p.calls.Add(1)
	if p.run != nil {
		return p.run(ctx, run, input)
	}
	return TurnResult{Outcome: "completed", Result: "answer"}, nil
}
func (*cursorProduct) Interrupt(context.Context, *Run) error { return nil }
func (*cursorProduct) Deliver(context.Context, DeliveryRequest, *Run) (DeliveryReceipt, error) {
	return DeliveryReceipt{Disposition: "written"}, nil
}
func (p *cursorProduct) Close(context.Context, SessionCloseRequest) error {
	if p.close != nil {
		p.close()
	}
	return nil
}

func cursorWire(t *testing.T, p *cursorProduct, observe ...func(*rpc.Request)) (*Worker, *rpc.Conn) {
	t.Helper()
	left, right := net.Pipe()
	w := NewWorker(p)
	w.context, w.cancel = context.WithCancel(context.Background())
	w.conn = rpc.New(left, true, func(ctx context.Context, r *rpc.Request) {
		for _, call := range observe {
			call(r)
		}
		w.handle(ctx, r)
	})
	var server *rpc.Conn
	server = rpc.New(right, false, func(_ context.Context, r *rpc.Request) {
		go func() {
			if r.Method == "turn.ready" {
				_ = server.Result(r, struct{}{})
			}
		}()
	})
	t.Cleanup(func() { w.cancel(); _ = w.conn.Close(); _ = server.Close() })
	return w, server
}
func cursorOpen(t *testing.T, c *rpc.Conn) {
	t.Helper()
	if err := c.Call(context.Background(), "session.open", OpenRequest{Name: "parent@label/leaf@local", Groups: []string{"g"}, Open: OpenOptions{}}, &OpenResult{}); err != nil {
		t.Fatal(err)
	}
}
func cursorExecute(c *rpc.Conn, session, id string) error {
	return c.Call(context.Background(), "turn.execute", protocol.ExecuteRequest{SessionID: session, RunID: id, Input: "work"}, &RunRef{})
}
func cursorWait(t *testing.T, c *rpc.Conn, id string) RunStatus {
	t.Helper()
	var got RunStatus
	if err := c.Call(context.Background(), "turn.wait", WaitRequest{SessionID: "native@local", RunID: id}, &got); err != nil {
		t.Fatal(err)
	}
	return got
}
func cursorCode(t *testing.T, err error, code int) {
	t.Helper()
	var failure *ProtocolError
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("got %v, want %d", err, code)
	}
}

func TestWorkerCursorCanonicalAndContiguous(t *testing.T) {
	p := &cursorProduct{}
	_, c := cursorWire(t, p)
	cursorOpen(t, c)
	cursorCode(t, cursorExecute(c, "foreign@local", "g/1"), protocol.InvalidFrame)
	cursorCode(t, cursorExecute(c, "native@local", "g/2"), protocol.InvalidFrame)
	if p.calls.Load() != 0 {
		t.Fatal("invalid admission reached native callback")
	}
	if err := cursorExecute(c, "native@local", "g/1"); err != nil {
		t.Fatal(err)
	}
	if got := cursorWait(t, c, "g/1"); got.State != "done" || got.SessionID != "native@local" {
		t.Fatalf("status %#v", got)
	}
	ref := RunRef{SessionID: "native@local", RunID: "g/1"}
	for range 2 {
		if err := c.Call(context.Background(), "turn.ack", ref, &struct{}{}); err != nil {
			t.Fatal(err)
		}
	}
	cursorCode(t, c.Call(context.Background(), "turn.ack", RunRef{SessionID: ref.SessionID, RunID: "g/2"}, &struct{}{}), protocol.UnknownSession)
	if err := cursorExecute(c, "native@local", "g/2"); err != nil {
		t.Fatal(err)
	}
	_ = cursorWait(t, c, "g/2")
}
func TestWorkerCursorPendingOpenRejectsSeed(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	p := &cursorProduct{open: func() { close(entered); <-release }}
	_, c := cursorWire(t, p)
	opened := make(chan error, 1)
	go func() {
		opened <- c.Call(context.Background(), "session.open", OpenRequest{Name: "label@name@local", Groups: []string{}, Open: OpenOptions{}}, &OpenResult{})
	}()
	<-entered
	seed := DeliveryRequest{MessageID: "m", From: DeliverySource{SessionID: "peer@local", Product: "peer", Groups: []string{}}, Body: "hello", RunID: "g/1"}
	cursorCode(t, c.Call(context.Background(), "message.deliver", seed, &DeliveryReceipt{}), protocol.Busy)
	close(release)
	if err := <-opened; err != nil {
		t.Fatal(err)
	}
	if p.calls.Load() != 0 {
		t.Fatal("pre-open seed reached native callback")
	}
	if err := cursorExecute(c, "native@local", "g/1"); err != nil {
		t.Fatal(err)
	}
	_ = cursorWait(t, c, "g/1")
}
func TestWorkerCursorInvalidReceiptClosesOriginalCall(t *testing.T) {
	reported := make(chan error, 1)
	p := &cursorProduct{run: func(_ context.Context, r *Run, _ RunInput) (TurnResult, error) {
		reported <- r.ReportDelivery(DeliveryReceipt{Disposition: "invented"}, nil)
		return TurnResult{Outcome: "completed"}, nil
	}}
	w, c := cursorWire(t, p)
	cursorOpen(t, c)
	seed := DeliveryRequest{MessageID: "m", From: DeliverySource{SessionID: "peer@local", Product: "peer", Groups: []string{}}, Body: "hello", RunID: "g/1"}
	if err := c.Call(context.Background(), "message.deliver", seed, &DeliveryReceipt{}); err == nil {
		t.Fatal("invalid receipt succeeded")
	}
	if err := <-reported; err == nil {
		t.Fatal("invalid receipt did not report encoding failure")
	}
	<-w.conn.Done()
}
func TestWorkerCursorChecksLargestReplyEnvelope(t *testing.T) {
	status := RunStatus{SessionID: "native@local", RunID: "g/1", State: "done", Result: &TurnResult{Outcome: "completed"}}
	empty, _ := protocol.ResultBytes(1, "turn.status", status)
	// JSON control escapes expand six bytes per character while remaining below
	// the native character limit. Fill the ID=1 frame exactly to its byte limit.
	size := protocol.MaxFrameBytes - len(empty)
	status.Result.Result = strings.Repeat("\x00", size/6) + strings.Repeat("x", size%6)
	if _, err := protocol.ResultBytes(1, "turn.status", status); err != nil {
		t.Fatalf("small envelope fixture: %v", err)
	}
	if _, err := protocol.ResultBytes(protocol.MaxRequestID, "turn.status", status); err == nil {
		t.Fatal("fixture must overflow largest ID envelope")
	}
	p := &cursorProduct{run: func(context.Context, *Run, RunInput) (TurnResult, error) { return *status.Result, nil }}
	_, c := cursorWire(t, p)
	cursorOpen(t, c)
	if err := cursorExecute(c, "native@local", "g/1"); err != nil {
		t.Fatal(err)
	}
	if got := cursorWait(t, c, "g/1"); got.State != "unavailable" || got.Result != nil {
		t.Fatalf("oversized retained result %#v", got)
	}
}

func TestWorkerCursorFIFOAndCapacity(t *testing.T) {
	p := &cursorProduct{}
	w, c := cursorWire(t, p)
	cursorOpen(t, c)
	for sequence := 1; sequence <= protocol.MaxOperations; sequence++ {
		id := "g/" + strconv.Itoa(sequence)
		if err := cursorExecute(c, "native@local", id); err != nil {
			t.Fatal(err)
		}
		_ = cursorWait(t, c, id)
	}
	cursorCode(t, cursorExecute(c, "native@local", "g/257"), protocol.Busy)
	cursorCode(t, c.Call(context.Background(), "turn.ack", RunRef{SessionID: "native@local", RunID: "g/2"}, &struct{}{}), protocol.Busy)
	var oldest RunStatus
	if err := c.Call(context.Background(), "turn.status", ReadRequest{SessionID: "native@local"}, &oldest); err != nil || oldest.RunID != "g/1" {
		t.Fatalf("oldest %#v %v", oldest, err)
	}
	if err := c.Call(context.Background(), "turn.ack", RunRef{SessionID: "native@local", RunID: "g/1"}, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := cursorExecute(c, "native@local", "g/257"); err != nil {
		t.Fatal(err)
	}
	_ = cursorWait(t, c, "g/257")
	w.mu.Lock()
	count := len(w.records)
	w.mu.Unlock()
	if count != protocol.MaxOperations {
		t.Fatalf("records %d", count)
	}
}

func TestWorkerCursorCloseSettlesAdmittedReader(t *testing.T) {
	release, entered, closeEntered, closeRelease := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	p := &cursorProduct{run: func(context.Context, *Run, RunInput) (TurnResult, error) {
		<-release
		return TurnResult{Outcome: "completed", Result: "retained"}, nil
	}, close: func() { close(closeEntered); <-closeRelease }}
	w, c := cursorWire(t, p, func(r *rpc.Request) {
		if r.Method == "turn.wait" {
			close(entered)
		}
	})
	cursorOpen(t, c)
	if err := cursorExecute(c, "native@local", "g/1"); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	var status RunStatus
	go func() {
		read <- c.Call(context.Background(), "turn.wait", WaitRequest{SessionID: "native@local", RunID: "g/1"}, &status)
	}()
	<-entered
	closing := make(chan error, 1)
	go func() {
		closing <- c.Call(context.Background(), "session.close", SessionCloseRequest{SessionID: "native@local"}, &struct{}{})
	}()
	close(release)
	if err := <-read; err != nil || status.State != "done" {
		t.Fatalf("read %#v %v", status, err)
	}
	<-closeEntered
	w.mu.Lock()
	count := w.waiters
	w.mu.Unlock()
	if count != 0 {
		t.Fatalf("Close raced %d readers", count)
	}
	cursorCode(t, c.Call(context.Background(), "turn.status", ReadRequest{SessionID: "native@local"}, &RunStatus{}), protocol.Busy)
	close(closeRelease)
	if err := <-closing; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerCloseEOFSettlesCapturedRun(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(strconv.FormatBool(terminal), func(t *testing.T) {
			captured := make(chan *Run, 1)
			release, ready := make(chan struct{}), make(chan struct{})
			p := &cursorProduct{run: func(_ context.Context, run *Run, _ RunInput) (TurnResult, error) {
				captured <- run
				<-release
				return TurnResult{Outcome: "completed"}, nil
			}}
			left, right := net.Pipe()
			w := NewWorker(p)
			closing, returned := make(chan struct{}), make(chan struct{})
			w.conn = rpc.New(left, true, func(ctx context.Context, request *rpc.Request) {
				if request.Method != "session.close" {
					w.handle(ctx, request)
					return
				}
				// Preserve the production close admission's captured slot/sentinel,
				// and observe the close routine itself returning after transport EOF.
				w.mu.Lock()
				slot := w.run
				w.run = &Run{}
				w.mu.Unlock()
				go func() { close(closing); w.close(ctx, request, slot, false); close(returned) }()
			})
			w.context, w.cancel = context.WithCancel(w.conn.Context())
			c := rpc.New(right, false, func(_ context.Context, request *rpc.Request) {
				if request.Method == "turn.ready" {
					close(ready) // Hold the real ready acknowledgement across EOF.
				}
			})
			t.Cleanup(func() { w.cancel(); _ = w.conn.Close(); _ = c.Close() })
			cursorOpen(t, c)
			if err := cursorExecute(c, "native@local", "g/1"); err != nil {
				t.Fatal(err)
			}
			run := <-captured
			if terminal {
				close(release)
				<-ready
			}
			result := make(chan error, 1)
			go func() {
				result <- c.Call(context.Background(), "session.close", SessionCloseRequest{SessionID: "native@local"}, &struct{}{})
			}()
			<-closing
			_ = c.Close()
			for name, channel := range map[string]<-chan struct{}{"captured Run.Done": run.Done(), "close routine return": returned} {
				select {
				case <-channel:
				case <-time.After(time.Second):
					t.Errorf("%s remained blocked after EOF", name)
				}
			}
			if !terminal {
				close(release)
			}
			if err := <-result; err == nil {
				t.Fatal("close crossing EOF succeeded")
			}
		})
	}
}
