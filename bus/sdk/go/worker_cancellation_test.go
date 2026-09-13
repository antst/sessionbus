// SPDX-License-Identifier: MIT
package sessionkit

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestWorkerCallBeforeServeIsUnavailable(t *testing.T) {
	w := NewWorker(&fakeProduct{})
	if err := w.Call(context.Background(), "session.list", SessionListRequest{}, &SessionListResult{}); err == nil {
		t.Fatal("unstarted worker fabricated a result")
	}
}

func TestWorkerShutdownAfterFailedDial(t *testing.T) {
	setEnvironment(t, "token", "")
	w := NewWorker(&fakeProduct{})
	w.dial = func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("daemon absent") }
	if err := w.Serve(context.Background()); err == nil {
		t.Fatal("missing daemon was accepted")
	}
	w.Shutdown()
	w.Shutdown()
	<-w.Closed()
}

func TestWorkerServeCancellationAfterHelloClosesProduct(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &fakeProduct{closeContext: make(chan error, 1)}
	h := startHarnessContext(t, ctx, p, true, true)
	defer h.Close()
	cancel()
	select {
	case <-p.worker.Closed():
	case <-time.After(time.Second):
		t.Fatal("Serve ignored cancellation after registration")
	}
	select {
	case err := <-p.closeContext:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close context = %v", err)
		}
	default:
		t.Fatal("opened product was not closed before Worker.Closed")
	}
}

func TestWorkerShutdownCancelsInitialDial(t *testing.T) {
	setEnvironment(t, "token", "")
	w := NewWorker(&fakeProduct{})
	entered := make(chan struct{})
	w.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	serving := make(chan error, 1)
	go func() { serving <- w.Serve(context.Background()) }()
	<-entered
	w.Shutdown()
	select {
	case err := <-serving:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled dial = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not cancel the initial dial")
	}
}

func TestWorkerCancellationClosesUnacknowledgedHello(t *testing.T) {
	setEnvironment(t, "token", "")
	w := NewWorker(&fakeProduct{})
	client, server := net.Pipe()
	defer server.Close()
	entered := make(chan struct{})
	w.dial = func(context.Context, string, string) (net.Conn, error) {
		close(entered)
		return client, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serving := make(chan error, 1)
	go func() { serving <- w.Serve(ctx) }()
	<-entered
	// The server never reads, so the hello write itself cannot finish.
	cancel()
	select {
	case err := <-serving:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unacknowledged hello = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close the startup transport")
	}
}

func TestWorkerShutdownBeforeServeAndSecondServe(t *testing.T) {
	setEnvironment(t, "token", "")
	w := NewWorker(&fakeProduct{})
	w.Shutdown()
	w.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if ctx.Err() == nil {
			t.Error("Shutdown was forgotten before Serve")
		}
		return nil, ctx.Err()
	}
	if err := w.Serve(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve after Shutdown = %v", err)
	}
	<-w.Closed()
	if err := w.Serve(context.Background()); err == nil {
		t.Fatal("Worker allowed a second Serve")
	}
}
