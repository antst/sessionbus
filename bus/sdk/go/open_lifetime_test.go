// SPDX-License-Identifier: MIT
package sessionkit

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
)

type heldOpenProduct struct {
	fakeProduct
	entered, release, closeEntered, closeRelease chan struct{}
	openFailure                                  error
}

func (p *heldOpenProduct) Open(context.Context, OpenRequest) (OpenResult, error) {
	atomic.AddInt32(&p.calls[1], 1)
	close(p.entered)
	<-p.release
	return OpenResult{SessionID: "native-owned"}, p.openFailure
}
func (p *heldOpenProduct) Close(context.Context, SessionCloseRequest) error {
	atomic.AddInt32(&p.calls[5], 1)
	close(p.closeEntered)
	<-p.closeRelease
	return nil
}
func TestWorkerJoinsOpenAdoptionOnEOF(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "late-success", true: "failed-open-cleanup"}[failure], func(t *testing.T) {
			setEnvironment(t, "token", "")
			p := &heldOpenProduct{entered: make(chan struct{}), release: make(chan struct{}), closeEntered: make(chan struct{}), closeRelease: make(chan struct{})}
			if failure {
				p.openFailure = errors.New("native open cleanup failed")
			}
			client, server := net.Pipe()
			w := NewWorker(p)
			w.dial = func(context.Context, string, string) (net.Conn, error) { return client, nil }
			hello := make(chan struct{})
			var daemon *rpc.Conn
			daemon = rpc.New(server, false, func(_ context.Context, r *rpc.Request) {
				go func() { _ = daemon.Result(r, struct{}{}); close(hello) }()
			})
			serving := make(chan error, 1)
			go func() { serving <- w.Serve(context.Background()) }()
			<-hello
			var releaseOpen, releaseClose sync.Once
			t.Cleanup(func() {
				releaseOpen.Do(func() { close(p.release) })
				releaseClose.Do(func() { close(p.closeRelease) })
				_ = daemon.Close()
				<-w.Closed()
			})
			opening := async(daemon, "session.open", OpenRequest{Name: "lane@local", Groups: []string{}, Open: OpenOptions{}}, &OpenResult{})
			<-p.entered
			_ = daemon.Close()
			<-w.conn.Done()
			// Closed must not become observable while this held native callback owns
			// successful adoption or failed-Open cleanup. The timer is only a test bound.
			select {
			case <-w.Closed():
				t.Error("Worker.Closed preceded in-flight Open completion")
			case <-time.After(30 * time.Millisecond):
			}
			releaseOpen.Do(func() { close(p.release) })
			if !failure {
				select {
				case <-p.closeEntered:
				case <-time.After(time.Second):
					t.Fatal("successful late Open was never closed")
				}
				select {
				case <-w.Closed():
					t.Fatal("Worker.Closed preceded product.Close completion")
				default:
				}
			}
			releaseClose.Do(func() { close(p.closeRelease) })
			<-serving
			<-opening
			expected := int32(1)
			if failure {
				expected = 0
			}
			if got := atomic.LoadInt32(&p.calls[5]); got != expected {
				t.Fatalf("Close calls=%d, want %d", got, expected)
			}
		})
	}
}
