// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestListSelfInfoTracksBoundPeerIdentityOutsideSelectedRows(t *testing.T) {
	_, socket := startDaemon(t)
	peer := connectPeer(t, socket, "self", "", "team")
	other := connectPeer(t, socket, "other", "Same name", "team")
	var listed protocol.SessionListResult
	must(t, peer.call("session.list", protocol.SessionListRequest{}, &listed))
	if len(listed.Sessions) != 2 || listed.SelfInfo == nil || listed.SelfInfo.SessionID != "self@local" || listed.SelfInfo.Name != "" {
		t.Fatalf("unnamed self: %#v", listed)
	}
	// Names need not distinguish callers. A filter selecting someone else must
	// still report the authenticated source, including its current title.
	must(t, peer.peer.Rehello(context.Background(), other.identity.Name, map[string]any{}))
	listed, err := peer.peer.Caller.List(context.Background(), protocol.SessionListRequest{SessionID: "other@local"})
	must(t, err)
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "other@local" || listed.SelfInfo == nil || listed.SelfInfo.SessionID != "self@local" || listed.SelfInfo.Name != "Same name@local" || listed.SelfInfo.Product != "fixture-client" {
		t.Fatalf("filtered self after rename: %#v", listed)
	}
	peer.identity.SessionID, peer.identity.Name, peer.identity.Product = "replacement", "", "replacement-product"
	peer.identity.Groups = []string{"changed"}
	must(t, peer.peer.Replace(context.Background(), peer.identity))
	listed, err = peer.peer.Caller.List(context.Background(), protocol.SessionListRequest{Host: "local"})
	must(t, err)
	if len(listed.Sessions) != 1 {
		t.Fatalf("replacement list: %#v", listed)
	}
	row := listed.Sessions[0]
	want := &protocol.SessionSelfInfo{SessionID: "replacement@local", Product: "replacement-product", Groups: row.Groups}
	if !reflect.DeepEqual(listed.SelfInfo, want) || row.SessionID != want.SessionID || row.Name != "" {
		t.Fatalf("replacement self: %#v, row: %#v", listed.SelfInfo, row)
	}
	// The complete public shape must not expose private ownership credentials.
	raw, err := json.Marshal(listed.SelfInfo)
	must(t, err)
	var fields map[string]json.RawMessage
	must(t, json.Unmarshal(raw, &fields))
	if len(fields) != 3 || fields["session_id"] == nil || fields["product"] == nil || fields["groups"] == nil {
		t.Fatalf("unnamed public identity fields: %s", raw)
	}
}

func TestDirectedListKeepsAdmissionSnapshotAcrossRehello(t *testing.T) {
	for _, remote := range []string{
		`{"sessions":[]}`,
		`{"sessions":[],"self_info":{"session_id":"wrong@beta","name":"Wrong@beta","product":"remote-product","groups":["wrong"]}}`,
	} {
		t.Run(remote, func(t *testing.T) {
			d, s, wire := reviewSession(t)
			d.host = "alpha"
			d.federation = &federationLink{inbox: make(chan any, 1)}
			hello := &protocol.PeerHello{Protocol: 1, SessionID: "self", Name: "Before", Product: "peer", Groups: []string{"team"}}
			item, _, _, ok := d.directory.installPeer(s, hello, d.host)
			if !ok {
				t.Fatal("install peer")
			}
			s.identity = item
			want := listSelfInfo(s.federationCaller())
			params := json.RawMessage(`{"host":"beta"}`)
			s.list(protocol.Frame{ID: 1, Method: "session.list", Params: params, Request: true}, &protocol.SessionListRequest{Host: "beta"})
			call := (<-d.federation.inbox).(federation.OutgoingCall)
			if call.Value.From.SessionID != want.SessionID || call.Value.From.Name != want.Name {
				t.Fatalf("forwarded source: %#v", call.Value.From)
			}
			// The owner processes rehello while the remote response is pending.
			hello.Name = "After"
			if _, _, _, ok = d.directory.installPeer(s, hello, d.host); !ok {
				t.Fatal("rehello")
			}
			call.Reply <- federation.Reply{Result: json.RawMessage(remote)}
			select {
			case event := <-s.inbox:
				s.consumeReply(event.(replyEvent))
			case <-time.After(time.Second):
				t.Fatal("remote list did not settle")
			}
			must(t, wire.SetReadDeadline(time.Now().Add(time.Second)))
			frame, err := readRawFrame(bufio.NewReader(wire))
			must(t, err)
			var listed protocol.SessionListResult
			must(t, protocol.UnmarshalResult("session.list", frame.Result, &listed))
			if !reflect.DeepEqual(listed.SelfInfo, want) || len(listed.Sessions) != 0 || s.federationCaller().Name != "After@alpha" {
				t.Fatalf("captured self: %#v, want %#v", listed, want)
			}
		})
	}
}
