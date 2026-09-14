// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func testTraceCaller() Caller {
	return Caller{SessionID: "parent@alpha", Name: "Parent@alpha", Product: "peer",
		PrivateGroup: "session:parent@alpha", Groups: []string{"session:parent@alpha"},
		OwnerLifetime: "parent-life", SourceAttachment: "alpha-attachment"}
}

func traceMessageCall(t *testing.T, daemon *testDaemon, copy *TraceDestination) Reply {
	t.Helper()
	params, err := protocol.EncodeParams("message.send", protocol.MessageSendRequest{Target: "child@beta", Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	reply := make(chan Reply, 1)
	request := PublicRequest{Method: "message.send", Params: params, MessageID: "message-one", Trace: copy == nil, TraceCopy: copy}
	daemon.inbox <- OutgoingCall{Value: Forward{From: Caller{SessionID: "parent@alpha", Name: "Parent@alpha", Product: "peer",
		PrivateGroup: "session:parent@alpha", Groups: []string{"session:parent@alpha"}, OwnerLifetime: "parent-life"}, Request: request}, Reply: reply}
	select {
	case value := <-reply:
		return value
	case <-time.After(time.Second):
		t.Fatal("trace call did not return")
		return Reply{}
	}
}

func TestTraceForwardingDowngradesLegacyDestinationWithoutChangingSend(t *testing.T) {
	_, address, secrets := testHub(t)
	seen := make(chan PublicRequest, 2)
	result := testTraceResult(t)
	beta := connectLegacyTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		seen <- call.Request
		return immediate(Reply{Result: result}), nil
	})
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) {
		t.Fatal("unexpected call to alpha")
		return nil, nil
	})
	_ = callHosts(t, beta)
	_ = callHosts(t, alpha)
	reply := traceMessageCall(t, alpha, nil)
	if reply.Error != nil || string(reply.Result) != string(result) || reply.Trace == nil || len(reply.Trace) != 0 {
		t.Fatalf("downgraded reply = %#v", reply)
	}
	if request := <-seen; request.Trace || request.TraceCopy != nil {
		t.Fatalf("legacy destination received trace metadata: %#v", request)
	}
	failed := traceMessageCall(t, alpha, &TraceDestination{SessionID: "child@beta", Lifetime: "child-life"})
	if failed.Error == nil || failed.Error.Code != protocol.UnsupportedTrace || len(failed.Error.Data) != 0 {
		t.Fatalf("legacy copy reply = %#v", failed)
	}
	if hosts := callHosts(t, alpha); len(hosts) != 1 || hosts[0] != "beta" {
		t.Fatalf("capability rejection closed a healthy link: %#v", hosts)
	}
}

func TestTraceForwardingCarriesOnlyLiveTypedRecipients(t *testing.T) {
	_, address, secrets := testHub(t)
	forged := false
	result := testTraceResult(t)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if !call.Request.Trace {
			t.Fatal("trace-capable destination did not receive collection marker")
		}
		owner := call.From
		if forged {
			owner.SourceAttachment = "stale-attachment"
		}
		return immediate(Reply{Result: result, Trace: []TraceRecipient{{Child: "child@beta", Target: "child@beta", Mode: "events", Version: "revision-one", Owner: owner}}}), nil
	})
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) {
		t.Fatal("unexpected call to alpha")
		return nil, nil
	})
	_ = callHosts(t, beta)
	_ = callHosts(t, alpha)
	reply := traceMessageCall(t, alpha, nil)
	if reply.Error != nil || len(reply.Trace) != 1 || reply.Trace[0].Child != "child@beta" || reply.Trace[0].Owner.SourceAttachment == "" {
		t.Fatalf("trace reply = %#v", reply)
	}
	forged = true
	reply = traceMessageCall(t, alpha, nil)
	if reply.Error != nil || reply.Trace == nil || len(reply.Trace) != 0 {
		t.Fatalf("stale attachment was not stripped: %#v", reply)
	}
}

func TestLegacyOriginCannotRequestTraceMetadata(t *testing.T) {
	_, address, secrets := testHub(t)
	seen := make(chan PublicRequest, 1)
	result := testTraceResult(t)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		seen <- call.Request
		reply := Reply{Result: result}
		if call.Request.Trace {
			reply.Trace = []TraceRecipient{}
		}
		return immediate(reply), nil
	})
	alpha := connectLegacyTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) {
		t.Fatal("unexpected call to alpha")
		return nil, nil
	})
	_ = callHosts(t, beta)
	_ = callHosts(t, alpha)
	reply := traceMessageCall(t, alpha, nil)
	if reply.Error != nil || string(reply.Result) != string(result) || reply.Trace != nil {
		t.Fatalf("legacy-origin reply = %#v", reply)
	}
	if request := <-seen; request.Trace || request.TraceCopy != nil {
		t.Fatalf("legacy origin sent trace metadata: %#v", request)
	}
}

func testTraceResult(t *testing.T) json.RawMessage {
	t.Helper()
	raw, err := protocol.EncodeResult("message.send", protocol.MessageSendResult{MessageID: "message-one",
		Deliveries: []protocol.MessageSendDelivery{{Target: "child@beta", SessionID: "child@beta", DeliveryID: "delivery-one", Disposition: "injected"}}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestTraceEnvelopeIsInternalAndStrict(t *testing.T) {
	recipient := TraceRecipient{Child: "child@beta", Target: "child@beta", Mode: "events", Version: "revision-one", Owner: testTraceCaller()}
	reply := Reply{Result: testTraceResult(t), Trace: []TraceRecipient{recipient}}
	public, err := ResponseBytes(1, reply)
	if err != nil {
		t.Fatal(err)
	}
	publicFrame, err := protocol.DecodeFrame(public[:len(public)-1])
	if err != nil || string(publicFrame.Result) != string(reply.Result) {
		t.Fatalf("public response = %s, %v", public, err)
	}
	internal, err := responseBytes(1, reply, true)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(internal[:len(internal)-1])
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := decodePendingReply(pendingCall{method: "message.send", trace: true, target: "beta"}, frame)
	if !ok || string(decoded.Result) != string(reply.Result) || len(decoded.Trace) != 1 || decoded.Trace[0].Target != recipient.Target {
		t.Fatalf("decoded trace reply = %#v, %v", decoded, ok)
	}
	if _, ok := decodePendingReply(pendingCall{method: "message.send"}, frame); ok {
		t.Fatal("internal trace envelope was accepted by an ordinary pending call")
	}
	plainFrame, _ := protocol.DecodeFrame(public[:len(public)-1])
	plain, ok := decodePendingReply(pendingCall{method: "message.send", trace: true, target: "beta"}, plainFrame)
	if !ok || plain.Trace == nil || len(plain.Trace) != 0 || string(plain.Result) != string(reply.Result) {
		t.Fatalf("strict plain overflow fallback = %#v, %v", plain, ok)
	}
}

func TestTraceEnvelopeOverflowPreservesOrdinaryResult(t *testing.T) {
	recipient := TraceRecipient{Child: "child@beta", Mode: "events", Version: "revision-one", Owner: testTraceCaller()}
	recipient.Owner.Groups = append(recipient.Owner.Groups, strings.Repeat("g", protocol.MaxFrameBytes))
	body, err := responseBytes(1, Reply{Result: testTraceResult(t), Trace: []TraceRecipient{recipient}}, true)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(body[:len(body)-1])
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := decodePendingReply(pendingCall{method: "message.send", trace: true, target: "beta"}, frame)
	if !ok || decoded.Trace == nil || len(decoded.Trace) != 0 {
		t.Fatalf("recipient overflow reply = %#v, %v", decoded, ok)
	}

	var result json.RawMessage
	for size := protocol.MaxFrameBytes; size > protocol.MaxFrameBytes-256; size-- {
		result, _ = protocol.EncodeResult("message.send", protocol.MessageSendResult{MessageID: strings.Repeat("m", size), Deliveries: []protocol.MessageSendDelivery{}})
		if _, err = ResponseBytes(1, Reply{Result: result}); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal("could not construct a maximal ordinary response")
	}
	body, err = responseBytes(1, Reply{Result: result, Trace: []TraceRecipient{}}, true)
	if err != nil {
		t.Fatal(err)
	}
	frame, err = protocol.DecodeFrame(body[:len(body)-1])
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok = decodePendingReply(pendingCall{method: "message.send", trace: true, target: "beta"}, frame)
	if !ok || decoded.Trace == nil || len(decoded.Trace) != 0 || string(decoded.Result) != string(result) {
		t.Fatalf("empty-envelope overflow reply = %#v, %v", decoded, ok)
	}
}

func TestTraceRecipientAndCopyValidation(t *testing.T) {
	valid := TraceRecipient{Child: "child@beta", Target: "requested@beta", Mode: "content", Version: "revision-one", Owner: testTraceCaller()}
	raw, _ := json.Marshal(valid)
	if _, err := decodeTraceRecipient(raw, "beta"); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*TraceRecipient){
		"wrong child host": func(value *TraceRecipient) { value.Child = "child@gamma" },
		"invalid child":    func(value *TraceRecipient) { value.Child = "@beta" },
		"long target":      func(value *TraceRecipient) { value.Target = string(make([]byte, 1025)) },
		"invalid mode":     func(value *TraceRecipient) { value.Mode = "all" },
		"missing version":  func(value *TraceRecipient) { value.Version = "" },
		"missing lifetime": func(value *TraceRecipient) { value.Owner.OwnerLifetime = "" },
		"missing attachment": func(value *TraceRecipient) {
			value.Owner.SourceAttachment = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			raw, _ := json.Marshal(value)
			if _, err := decodeTraceRecipient(raw, "beta"); err == nil {
				t.Fatalf("accepted %#v", value)
			}
		})
	}

	params, _ := protocol.EncodeParams("message.send", protocol.MessageSendRequest{Target: "child@beta", Message: "copy"})
	request := PublicRequest{Method: "message.send", Params: params, MessageID: "message-one", TraceCopy: &TraceDestination{SessionID: "child@beta", Lifetime: "child-life"}}
	rawRequest, _ := json.Marshal(request)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(rawRequest, &fields)
	if err := validateTraceRequest(request, fields); err != nil {
		t.Fatal(err)
	}
	request.Trace = true
	if err := validateTraceRequest(request, fields); err == nil {
		t.Fatal("accepted trace collection and trace copy together")
	}
}

func TestExplicitTraceControlsRequireCapability(t *testing.T) {
	for _, mode := range []string{"off", "events", "content"} {
		params, _ := protocol.EncodeParams("lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "child", Product: "peer", Open: &protocol.OpenOptions{}, Trace: mode})
		if !requestRequiresTrace(PublicRequest{Method: "lane.spawn", Params: params}) {
			t.Fatalf("lane trace mode %q did not require trace capability", mode)
		}
	}
	params, _ := protocol.EncodeParams("lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "child", Product: "peer", Open: &protocol.OpenOptions{}})
	if requestRequiresTrace(PublicRequest{Method: "lane.spawn", Params: params}) {
		t.Fatal("omitted lane trace mode required trace capability")
	}
}

func TestTraceBoundsAndPresenceAreStrict(t *testing.T) {
	recipient := TraceRecipient{Child: "child@beta", Mode: "events", Version: "revision-one", Owner: testTraceCaller()}
	tooMany := make([]TraceRecipient, MaxTraceRecipients+1)
	for index := range tooMany {
		tooMany[index] = recipient
		tooMany[index].Target = string(rune(index + 1))
	}
	if _, err := encodeTraceReply(Reply{Result: testTraceResult(t), Trace: tooMany}); err == nil {
		t.Fatal("encoded too many trace recipients")
	}
	for _, raw := range []string{
		`{"result":{"message_id":"one","deliveries":[]}}`,
		`{"result":{"message_id":"one","deliveries":[]},"trace":null}`,
		`{"result":{"message_id":"one","deliveries":[]},"trace":{},"extra":true}`,
	} {
		if _, err := decodeTraceReply("message.send", []byte(raw), "beta"); err == nil {
			t.Fatalf("accepted trace envelope %s", raw)
		}
	}
	params, _ := protocol.EncodeParams("message.send", protocol.MessageSendRequest{Target: "child@beta", Message: "hello"})
	request := PublicRequest{Method: "message.send", Params: params, MessageID: "one"}
	if err := validateTraceRequest(request, map[string]json.RawMessage{"trace": json.RawMessage("false")}); err == nil {
		t.Fatal("accepted explicit false trace marker")
	}
	if err := validateTraceRequest(request, map[string]json.RawMessage{"trace_copy": json.RawMessage("null")}); err == nil {
		t.Fatal("accepted null trace copy marker")
	}
}
