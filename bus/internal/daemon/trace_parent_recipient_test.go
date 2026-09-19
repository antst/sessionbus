// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func traceOriginal(t *testing.T, recipient *peerClient, from, body string) protocol.DeliveryRequest {
	t.Helper()
	select {
	case got := <-recipient.deliveries:
		if got.From.SessionID != from || got.Body != body {
			t.Fatalf("original delivery = %+v", got)
		}
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("original did not arrive")
	}
	return protocol.DeliveryRequest{}
}

func TestParentTraceSoleRecipientIsNotCopied(t *testing.T) {
	for _, mode := range []string{"events", "content"} {
		t.Run(mode, func(t *testing.T) {
			d, parent := traceFixture(t)
			child := traceSpawn(t, parent, "child", mode, false)
			other := connectPeer(t, d.config.SocketPath, "other", "other", "team")
			for _, tc := range []struct {
				name    string
				targets []string
				copy    bool
			}{
				{"canonical", []string{"parent@local"}, false},
				{"name", []string{"parent"}, false},
				{"aliases", []string{"parent", "parent@local"}, false},
				{"parent-and-other", []string{"parent@local", "other@local"}, true},
				{"parent-and-unresolved", []string{"parent@local", "missing"}, true},
				{"other", []string{"other@local"}, true},
				{"unresolved", []string{"missing"}, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result := traceChildSend(t, d, child, protocol.MessageSendRequest{Targets: tc.targets, Message: tc.name})
					for _, delivery := range result.Deliveries {
						switch delivery.SessionID {
						case "parent@local":
							traceOriginal(t, parent, child, tc.name)
						case "other@local":
							traceOriginal(t, other, child, tc.name)
						case "":
							if delivery.Disposition != "rejected" {
								t.Fatal(delivery)
							}
							continue
						default:
							t.Fatal(delivery)
						}
						if delivery.Disposition != "injected" {
							t.Fatal(delivery)
						}
					}
					if tc.copy {
						_, copy := traceReceive(t, parent)
						if copy.MessageID != result.MessageID || len(copy.Deliveries) != len(result.Deliveries) {
							t.Fatal(copy)
						}
					} else if len(result.Deliveries) != 1 {
						t.Fatal(result)
					}
					traceIdle(t, d)
					traceAbsent(t, parent)
				})
			}
			d.directory.mu.Lock()
			dropped := d.traceDropped
			d.directory.mu.Unlock()
			if dropped != 0 {
				t.Fatalf("intentional omission counted as loss: %d", dropped)
			}
		})
	}
}

func TestParentTraceCompletionPointerSoleRecipient(t *testing.T) {
	for _, mode := range []string{"events", "content"} {
		for _, elsewhere := range []bool{false, true} {
			name := mode + "/parent"
			if elsewhere {
				name = mode + "/other"
			}
			t.Run(name, func(t *testing.T) {
				d, parent := traceFixture(t)
				recipient := parent
				persistent := elsewhere
				notifyTarget := ""
				if elsewhere {
					recipient = connectPeer(t, d.config.SocketPath, "other", "other", "team")
					notifyTarget = "other@local"
				}
				var child protocol.LaneSpawnResult
				must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}, Trace: mode, Persistent: &persistent, NotifyTarget: notifyTarget, ExtraGroups: []string{"team"}}, &child))
				var completed protocol.RunStatus
				must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: child.SessionID, Input: "ordinary explicit run"}, &completed))
				select {
				case pointer := <-recipient.deliveries:
					if pointer.From.SessionID != child.SessionID || !strings.HasPrefix(pointer.Body, "Lane "+child.SessionID+" run ") {
						t.Fatal(pointer)
					}
					if elsewhere {
						_, copy := traceReceive(t, parent)
						if copy.MessageID != pointer.MessageID || len(copy.Deliveries) != 1 || copy.Deliveries[0].SessionID != "other@local" {
							t.Fatal(copy)
						}
					}
				case <-time.After(3 * time.Second):
					t.Fatal("no completion pointer")
				}
				traceIdle(t, d)
				traceAbsent(t, parent)
			})
		}
	}
}

func TestParentTraceFederatedSoleRecipient(t *testing.T) {
	daemons, parent := traceFederation(t)
	other := connectPeer(t, daemons["gamma"].config.SocketPath, "other", "other", "team")
	for _, mode := range []string{"events", "content"} {
		child := traceRemoteSpawn(t, parent, "beta", "child-"+mode, mode)
		result := traceChildSend(t, daemons["beta"], child, protocol.MessageSendRequest{Target: "parent@alpha", Message: mode})
		traceOriginal(t, parent, child, mode)
		if len(result.Deliveries) != 1 || result.Deliveries[0].SessionID != "parent@alpha" || result.Deliveries[0].Disposition != "injected" {
			t.Fatal(result)
		}
		for _, d := range daemons {
			traceIdle(t, d)
		}
		traceAbsent(t, parent)
		result = traceChildSend(t, daemons["beta"], child, protocol.MessageSendRequest{Targets: []string{"parent@alpha", "other@gamma"}, Message: "fanout"})
		traceOriginal(t, parent, child, "fanout")
		traceOriginal(t, other, child, "fanout")
		_, copy := traceReceive(t, parent)
		if copy.MessageID != result.MessageID || len(copy.Deliveries) != 2 {
			t.Fatal(copy)
		}
		for _, d := range daemons {
			traceIdle(t, d)
		}
		traceAbsent(t, parent)
	}
}

func TestParentTraceSoleRecipientDoesNotSuppressOtherParent(t *testing.T) {
	d, grandparent := traceFixture(t)
	parent := traceSpawn(t, grandparent, "parent-lane", "content", false)
	child := traceSpawn(t, grandparent, "child", "content", false)
	// Establish the same direct-parent topology as the existing nested trace
	// fixture, using the real lanes' live ownership tokens.
	d.directory.mu.Lock()
	d.directory.entries[child].parent = d.directory.entries[parent].lifetime
	d.directory.mu.Unlock()
	result := traceChildSend(t, d, child, protocol.MessageSendRequest{Target: parent, Message: "to my parent"})
	if len(result.Deliveries) != 1 || result.Deliveries[0].Disposition != "injected" {
		t.Fatal(result)
	}
	_, copy := traceReceive(t, grandparent)
	if copy.MessageID != result.MessageID || len(copy.MatchedChildren) != 1 || copy.MatchedChildren[0] != parent {
		t.Fatal(copy)
	}
	traceIdle(t, d)
	traceAbsent(t, grandparent)
}

func TestParentTraceFederatedCompletionPointer(t *testing.T) {
	daemons, parent := traceFederation(t)
	other := connectPeer(t, daemons["gamma"].config.SocketPath, "other", "other", "team")
	for _, elsewhere := range []bool{false, true} {
		target, name, recipient := "", "to-parent", parent
		if elsewhere {
			target, name, recipient = "other@gamma", "to-other", other
		}
		var child protocol.LaneSpawnResult
		must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: name, Host: "beta", Product: "fixture-worker", Open: &protocol.OpenOptions{}, Trace: "content", Persistent: &elsewhere, NotifyTarget: target, ExtraGroups: []string{"team"}}, &child))
		var completed protocol.RunStatus
		must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: child.SessionID, Input: "ordinary remote run"}, &completed))
		select {
		case pointer := <-recipient.deliveries:
			if pointer.From.SessionID != child.SessionID || !strings.HasPrefix(pointer.Body, "Lane "+child.SessionID+" run ") {
				t.Fatal(pointer)
			}
			if elsewhere {
				_, copy := traceReceive(t, parent)
				if copy.MessageID != pointer.MessageID || len(copy.Deliveries) != 1 || copy.Deliveries[0].SessionID != target {
					t.Fatal(copy)
				}
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no remote completion pointer")
		}
		for _, d := range daemons {
			traceIdle(t, d)
		}
		traceAbsent(t, parent)
	}
}
