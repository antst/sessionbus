// SPDX-License-Identifier: MIT

// Package sessionkit is the product-agnostic Go implementation of Sessionbus.
package sessionkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type WorkerCallbacks interface {
	Hello(context.Context) (HelloDescription, error)
	Open(context.Context, OpenRequest) (OpenResult, error)
	Run(context.Context, *Run, RunInput) (TurnResult, error)
	Interrupt(context.Context, *Run) error
	Deliver(context.Context, DeliveryRequest, *Run) (DeliveryReceipt, error)
	Close(context.Context, SessionCloseRequest) error
}

// RunInput is exactly one caller input or an idle delivery seed.
type RunInput struct {
	Text     *string
	Delivery *DeliveryRequest
}

type Run struct {
	receipt     func(DeliveryReceipt, error) error
	receiptOnce sync.Once

	Native      any
	context     context.Context
	cancel      context.CancelFunc
	done        <-chan struct{}
	finish      context.CancelFunc
	admitted    chan struct{}
	admit       sync.Once
	interrupted atomic.Bool
}

func (r *Run) Interrupted() bool             { return r.interrupted.Load() }
func (r *Run) Done() <-chan struct{}         { return r.done }
func (r *Run) Admitted()                     { r.admit.Do(func() { close(r.admitted) }) }
func (r *Run) AdmittedDone() <-chan struct{} { return r.admitted }

// ReportDelivery answers the original delivery RPC once. It may block on the
// transport write; adapters must not call it while holding their native reader lock.
func (r *Run) ReportDelivery(value DeliveryReceipt, failure error) error {
	if r.receipt == nil {
		return errors.New("run has no delivery seed")
	}
	called := false
	var err error
	r.receiptOnce.Do(func() { called = true; err = r.receipt(value, failure) })
	if !called {
		return errors.New("delivery receipt already reported")
	}
	return err
}

type Worker struct {
	sessionID    string
	records      []*workerRecord
	changed      chan struct{}
	generation   string
	lastSequence uint64
	acknowledged uint64
	waiters      int

	product      WorkerCallbacks
	caller       *Caller
	dial         func(context.Context, string, string) (net.Conn, error)
	mu           sync.Mutex
	conn         *rpc.Conn
	context      context.Context
	cancel       context.CancelFunc
	run          *Run
	closeRequest SessionCloseRequest
	opened       atomic.Bool
	once         sync.Once
	closed       chan struct{}
}

func NewWorker(product WorkerCallbacks) *Worker {
	worker := &Worker{product: product, dial: (&net.Dialer{}).DialContext, closed: make(chan struct{}), changed: make(chan struct{})}
	worker.caller = NewCaller(func(ctx context.Context, method string, params any) (result json.RawMessage, err error) {
		err = worker.Call(ctx, method, params, &result)
		return
	})
	return worker
}

func (w *Worker) Closed() <-chan struct{} { return w.closed }
func (w *Worker) Caller() *Caller         { return w.caller }

func (w *Worker) Serve(ctx context.Context) error {
	defer close(w.closed)
	endpoint, token, err := sessionEnvironment(true)
	if err != nil {
		return err
	}
	hello, err := w.product.Hello(ctx)
	if err != nil {
		return err
	}
	fd, err := w.dial(ctx, "unix", endpoint)
	if err != nil {
		return err
	}
	w.conn = rpc.New(fd, true, w.handle)
	w.context, w.cancel = context.WithCancel(w.conn.Context())
	request := protocol.WorkerHello{Protocol: 1, LaunchToken: token, HelloDescription: hello}
	if err = w.conn.Call(ctx, "session.hello", request, &struct{}{}); err == nil {
		<-w.conn.Done()
		err = rpc.ErrClosed
	}
	_ = w.conn.Close()
	w.mu.Lock()
	if w.run != nil && w.run.finish != nil {
		w.run.finish()
	}
	w.mu.Unlock()
	w.closeProduct(w.conn.Context())
	return err
}

func (w *Worker) Call(ctx context.Context, method string, params, result any) error {
	return w.conn.Call(ctx, method, params, result)
}

func (w *Worker) Shutdown() { _ = w.conn.Close() }

func (w *Worker) handle(ctx context.Context, request *rpc.Request) {
	switch request.Method {
	case "session.superseded":
		go func() { w.reply(w.conn.Result(request, struct{}{})); _ = w.conn.Close() }()
	case "session.open":
		go w.open(w.context, request)
	case "turn.execute":
		w.execute(request, nil)
	case "turn.status", "turn.wait":
		w.readRun(request)
	case "turn.ack":
		w.ackRun(request)
	case "turn.interrupt":
		w.mu.Lock()
		if w.run == nil {
			w.mu.Unlock()
			go w.answer(request, nil, protocol.NotRunning)
			return
		}
		slot := w.run
		if slot.context != nil && slot.context.Err() != nil {
			w.mu.Unlock()
			go w.answer(request, nil, protocol.NotRunning)
			return
		}
		call := slot.context != nil && slot.interrupted.CompareAndSwap(false, true)
		w.mu.Unlock()
		go w.interrupt(request, slot, call)
	case "message.deliver":
		delivery := request.Params.(*DeliveryRequest)
		if delivery.RunID != "" {
			w.execute(request, delivery)
			return
		}
		w.mu.Lock()
		slot := w.run
		closing := slot != nil && slot.done == nil
		w.mu.Unlock()
		if closing {
			go w.answer(request, DeliveryReceipt{Disposition: "rejected", Reason: "closing"}, 0)
			return
		}
		go w.deliver(w.context, request, slot)
	case "session.close":
		w.mu.Lock()
		slot := w.run
		if slot != nil && slot.done == nil {
			w.mu.Unlock()
			_ = w.conn.Close()
			return
		}
		w.run = &Run{}
		w.run.interrupted.Store(true)
		w.closeRequest = *request.Params.(*SessionCloseRequest)
		interrupt := slot != nil && slot.context.Err() == nil && slot.interrupted.CompareAndSwap(false, true)
		w.mu.Unlock()
		go w.close(ctx, request, slot, interrupt)
	}
}

func (w *Worker) open(ctx context.Context, request *rpc.Request) {
	result, err := w.product.Open(ctx, *request.Params.(*OpenRequest))
	if err != nil {
		w.reply(w.conn.Error(request, protocol.SpawnFailed, map[string]any{"stderr_tail": []string{err.Error()}}))
		return
	}
	w.sessionID = result.SessionID
	if _, host, ok := strings.Cut(request.Params.(*OpenRequest).Name, "@"); ok {
		w.sessionID += "@" + host
	}
	w.opened.Store(true)
	w.reply(w.conn.Result(request, result))
}

func (w *Worker) interrupt(request *rpc.Request, run *Run, call bool) {
	if call {
		callbackError("interrupt", w.nativeInterrupt(run))
	}
	w.reply(w.conn.Result(request, struct{}{}))
}

func (w *Worker) deliver(ctx context.Context, request *rpc.Request, run *Run) {
	receipt, err := w.product.Deliver(ctx, *request.Params.(*DeliveryRequest), run)
	w.reply(w.deliveryReply(request, receipt, err))
}

func (w *Worker) deliveryReply(request *rpc.Request, receipt DeliveryReceipt, err error) error {
	var failure *ProtocolError
	if errors.As(err, &failure) && failure.Code == protocol.Internal {
		return w.conn.Error(request, failure.Code, failure.Data)
	}
	if err != nil {
		receipt = DeliveryReceipt{Disposition: "rejected", Reason: err.Error()}
	}
	return w.conn.Result(request, receipt)
}

func (w *Worker) close(ctx context.Context, request *rpc.Request, slot *Run, interrupt bool) {
	if interrupt {
		go func() {
			callbackError("interrupt", w.nativeInterrupt(slot))
		}()
	}
	if slot != nil {
		<-slot.done
	}
	w.cancel()
	w.closeProduct(ctx)
	w.reply(w.conn.Result(request, struct{}{}))
	_ = w.conn.Close()
}

func (w *Worker) nativeInterrupt(run *Run) error {
	if run.context.Err() != nil {
		return nil //nolint:nilerr // An already-cancelled run has nothing left to interrupt.
	}
	return w.product.Interrupt(run.context, run)
}

func (w *Worker) answer(request *rpc.Request, value any, code int) {
	if code != 0 {
		w.reply(w.conn.Error(request, code, nil))
	} else {
		w.reply(w.conn.Result(request, value))
	}
}

func (w *Worker) closeProduct(ctx context.Context) {
	w.once.Do(func() {
		w.mu.Lock()
		request := w.closeRequest
		w.mu.Unlock()
		if w.opened.Load() {
			callbackError("close", w.product.Close(ctx, request))
		}
	})
}

func callbackError(callback string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "sessionbus: product %s: %q\n", callback, err.Error())
	}
}

func (w *Worker) reply(err error) {
	if err != nil {
		_ = w.conn.Close()
	}
}

func sessionEnvironment(worker bool) (string, string, error) {
	token, ok := os.LookupEnv("SESSIONBUS_LAUNCH_TOKEN")
	key, endpoint := os.Getenv("SESSIONBUS_LOCAL_KEY"), Socket()
	for _, name := range []string{"SESSIONBUS_LAUNCH_TOKEN", "SESSIONBUS_LOCAL_KEY", "SESSIONBUS_SOCKET"} {
		_ = os.Unsetenv(name)
	}
	if worker && (!ok || token == "") {
		return "", "", errors.New("launch token is required")
	}
	if endpoint == "" {
		return "", "", errors.New("sessionbus socket is required")
	}
	if key != "" {
		return "", "", errors.New("local key transport not implemented in this build")
	}
	return endpoint, token, nil
}

func truncate(text string) (string, bool) {
	characters := []rune(text)
	return string(characters[:min(len(characters), protocol.MaxTextRunes)]), len(characters) > protocol.MaxTextRunes
}
