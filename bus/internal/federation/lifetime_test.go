// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestLifetimeControlsFollowAuthenticatedSpawn(t *testing.T) {
	_, address, secrets := testHub(t)
	events := make(chan any, 8)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		events <- call
		return immediate(Reply{Result: []byte(`{"session_id":"lane@beta"}`)}), nil
	}, func(event LifetimeEvent) { events <- event })
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, alpha)
	if hosts := callHosts(t, beta); len(hosts) != 1 {
		t.Fatalf("hosts %v", hosts)
	}
	caller := Caller{SessionID: "owner@alpha", Name: "owner@alpha", Product: "peer", Groups: []string{"team", "session:owner@alpha"}, PrivateGroup: "session:owner@alpha", OwnerLifetime: "life"}
	raw, _ := json.Marshal(protocol.LaneSpawnRequest{Host: "beta", Name: "child", Product: "worker", Open: &protocol.OpenOptions{}})
	spawnReply, endReply := make(chan Reply, 1), make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "lane.spawn", Params: raw}}, Reply: spawnReply}
	alpha.inbox <- OwnerEndCall{Value: OwnerEnd{Host: "beta", SessionID: caller.SessionID, Lifetime: caller.OwnerLifetime}, Reply: endReply}
	first := lifetimeEvent(t, events).(IncomingCall)
	second := lifetimeEvent(t, events).(LifetimeEvent)
	if first.From.SourceAttachment == "" || first.From.OwnerLifetime != "life" || second.Owner == nil || second.Owner.Source != "alpha" || second.Owner.Attachment != first.From.SourceAttachment || second.Owner.Lifetime != "life" {
		t.Fatalf("bindings %#v %#v", first, second)
	}
	if reply := <-spawnReply; reply.Error != nil {
		t.Fatal(reply.Error)
	}
	if reply := <-endReply; reply.Error != nil {
		t.Fatal(reply.Error)
	}
	alpha.cancel()
	ended := lifetimeEvent(t, events).(LifetimeEvent)
	if ended.Host == nil || ended.Host.Host != "alpha" || ended.Host.Attachment != first.From.SourceAttachment {
		t.Fatalf("host end %#v", ended)
	}
	replacement := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	replacement.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "lane.spawn", Params: raw}}, Reply: spawnReply}
	next := lifetimeEvent(t, events).(IncomingCall)
	if next.From.SourceAttachment == first.From.SourceAttachment {
		t.Fatal("reconnected host reused attachment epoch")
	}
	if reply := <-spawnReply; reply.Error != nil {
		t.Fatal(reply.Error)
	}
}
func lifetimeEvent(t *testing.T, events <-chan any) any {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("lifetime event missing")
		return nil
	}
}

func TestLifetimeControlRejectsForgedSourceAndHostEnd(t *testing.T) {
	for _, value := range []struct {
		method string
		params any
	}{
		{ownerEndMethod, OwnerEnd{Host: "beta", SessionID: "owner@other", Lifetime: "life"}},
		{ownerEndMethod, OwnerEnd{Host: "beta", SessionID: "owner@alpha", Lifetime: "life", Source: "alpha"}},
		{ownerEndMethod, OwnerEnd{Host: "beta", SessionID: "owner@alpha", Lifetime: "life", Attachment: "forged"}},
		{hostEndMethod, HostEnd{Host: "beta", Attachment: "forged"}},
	} {
		raw, _ := json.Marshal(value.params)
		link := &hostLink{host: "alpha", ctx: context.Background()}
		last := int64(0)
		err := link.frame(conn.Frame{Value: protocol.Frame{ID: 1, Request: true, Method: value.method, Params: raw}}, map[int64]pendingCall{}, &last)
		if err == nil {
			t.Fatalf("accepted forged %s %#v", value.method, value.params)
		}
	}
}
func TestLifetimeQueuedControlsSettleOnLoss(t *testing.T) {
	link := &hostLink{inbox: make(chan any, 2)}
	reply := make(chan Reply, 1)
	link.inbox <- lifetimeCall{Method: ownerEndMethod, Value: OwnerEnd{}, Reply: reply}
	link.settleQueued()
	if got := <-reply; got.Error == nil || got.Error.Code != protocol.ForwardLost {
		t.Fatalf("queued control %#v", got)
	}
}
