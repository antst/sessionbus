// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type reviewEOFConn struct {
	net.Conn
	eof chan struct{}
}

func (c *reviewEOFConn) Read(body []byte) (int, error) {
	n, err := c.Conn.Read(body)
	if err != nil {
		close(c.eof) // the connection reader terminates after its first error
	}
	return n, err
}

func TestReviewFederationRetiresAndSettlesAcceptedQueue(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	local, peer := net.Pipe()
	fd := &reviewEOFConn{Conn: local, eof: make(chan struct{})}
	if err := d.StartFederation(context.Background(), fd, io.Discard); err != nil {
		t.Fatal(err)
	}
	d.directory.mu.Lock()
	link := d.federation
	peer.Close()
	<-fd.eof
	// This is the same admission boundary used by postFederation. Retirement
	// cannot clear the attachment until this accepted call has been enqueued.
	reply := make(chan federation.Reply, 1)
	link.inbox <- federation.HostsCall{Reply: reply}
	d.directory.mu.Unlock()
	joined := make(chan struct{})
	go func() { d.group.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("federation owner did not retire")
	}
	select {
	case value := <-reply:
		if value.Error == nil || value.Error.Code != protocol.ForwardLost {
			t.Fatalf("wrong terminal: %#v", value)
		}
	default:
		t.Fatal("accepted call was not settled at retirement")
	}
	if code := d.directory.postFederation(federation.HostsCall{Reply: make(chan federation.Reply, 1)}); code != protocol.UnknownHost {
		t.Fatalf("retired attachment still admitted calls: %d", code)
	}
}
