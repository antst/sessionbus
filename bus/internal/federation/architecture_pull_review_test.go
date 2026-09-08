// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
)

func TestReviewRegistryShutdownClosesQueuedAcceptedSocket(t *testing.T) {
	registry := NewRegistry(io.Discard)
	local, peer := net.Pipe()
	if !registry.Accept(local, &tls.Config{}) {
		t.Fatal("socket was not accepted")
	}
	registry.Close()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("accepted socket remained open")
	} else if failure, ok := err.(net.Error); ok && failure.Timeout() {
		t.Fatal("accepted socket reached a deadline instead of closing")
	}
	_ = peer.Close()
}

func TestReviewRejectedHostDrainsStartedReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	local, peer := net.Pipe()
	link := newHostLink(ctx, "alpha", local, local, make(chan registryEvent), io.Discard)
	for index := 0; index < cap(link.inbox); index++ {
		link.inbox <- conn.Frame{}
	}
	cancel()
	stopped := make(chan struct{})
	go func() { link.run(false); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		_ = peer.Close()
		t.Fatal("rejected host owner did not drain and join its reader")
	}
	_ = peer.Close()
}
