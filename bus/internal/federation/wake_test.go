// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestCompletionProvenanceIsPrivateAndStrict(t *testing.T) {
	params := json.RawMessage(`{"target":"child@beta","message":"notice"}`)
	if _, err := protocol.DecodeParams("message.send", json.RawMessage(`{"target":"child@beta","message":"notice","completion":true}`)); err == nil {
		t.Fatal("public sender forged completion provenance")
	}
	value := Forward{From: testTraceCaller(), Request: PublicRequest{Method: "message.send", Params: params, MessageID: "m", Completion: true}}
	raw, _ := json.Marshal(value)
	decoded, _, err := decodeForward(raw, "alpha")
	if err != nil || !decoded.Request.Completion {
		t.Fatalf("private provenance lost: %+v, %v", decoded, err)
	}
	for _, invalid := range []json.RawMessage{[]byte(`false`), []byte(`null`), []byte(`1`), []byte(`"true"`)} {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		var request map[string]json.RawMessage
		_ = json.Unmarshal(fields["request"], &request)
		request["completion"] = invalid
		fields["request"], _ = json.Marshal(request)
		bad, _ := json.Marshal(fields)
		if _, _, err := decodeForward(bad, "alpha"); err == nil {
			t.Fatalf("invalid completion marker accepted: %s", invalid)
		}
	}
	for _, invalid := range []json.RawMessage{[]byte(`{"group":"team","host":"beta","message":"notice"}`), []byte(`{"targets":["a@beta","b@beta"],"message":"notice"}`)} {
		value.Request.Params = invalid
		bad, _ := json.Marshal(value)
		if _, _, err := decodeForward(bad, "alpha"); err == nil {
			t.Fatal("completion broadcast accepted")
		}
	}
}

func TestCompletionCaseVariantCannotBypassValidation(t *testing.T) {
	for _, name := range []string{"Completion", "COMPLETION", "cOmPlEtIoN"} {
		for _, shape := range []struct {
			name, request string
		}{
			{"single", `"method":"message.send","params":{"target":"child@beta","message":"notice"},"message_id":"m"`},
			{"group", `"method":"message.send","params":{"group":"team","host":"beta","message":"notice"},"message_id":"m"`},
			{"multiple", `"method":"message.send","params":{"targets":["a@beta","b@beta"],"message":"notice"},"message_id":"m"`},
			{"method", `"method":"session.list","params":{}`},
			{"trace", `"method":"message.send","params":{"target":"child@beta","message":"notice"},"message_id":"m","trace_copy":{"session_id":"child@beta","lifetime":"generation"}`},
		} {
			t.Run(name+"/"+shape.name, func(t *testing.T) {
				caller, err := json.Marshal(testTraceCaller())
				if err != nil {
					t.Fatal(err)
				}
				raw := json.RawMessage(`{"from":` + string(caller) + `,"request":{` + shape.request + `,"` + name + `":true}}`)
				if _, _, err := decodeForward(raw, "alpha"); err == nil {
					t.Fatalf("case-variant completion bypassed validation: %s", raw)
				}
			})
		}
	}
}

func TestCompletionForwardingPreservesOriginOrRefusesLegacyLink(t *testing.T) {
	for _, legacy := range []string{"", "alpha", "beta"} {
		t.Run("legacy="+legacy, func(t *testing.T) {
			_, address, secrets := testHub(t)
			seen := make(chan PublicRequest, 2)
			admit := func(call IncomingCall) (Wait, error) {
				seen <- call.Request
				return immediate(Reply{Result: testTraceResult(t)}), nil
			}
			connect := func(host string) *testDaemon {
				configuration, err := ClientTLS(host, secrets[host])
				if err != nil {
					t.Fatal(err)
				}
				if host == legacy {
					configuration.NextProtos = []string{TraceALPN, RosterALPN}
				}
				return connectTestDaemonTLS(t, address, host, configuration, admit)
			}
			beta, alpha := connect("beta"), connect("alpha")
			_ = callHosts(t, beta)
			_ = callHosts(t, alpha)
			caller := testTraceCaller()
			caller.SourceAttachment = ""
			for _, completion := range []bool{true, false} {
				reply := make(chan Reply, 1)
				alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "message.send", Params: json.RawMessage(`{"target":"child@beta","message":"notice"}`), MessageID: "m", Completion: completion}}, Reply: reply}
				select {
				case got := <-reply:
					if completion && legacy != "" {
						if got.Error == nil || got.Error.Code != protocol.ForwardLost {
							t.Fatalf("legacy reply: %+v", got)
						}
						select {
						case got := <-seen:
							t.Fatalf("legacy link received marked request: %+v", got)
						default:
						}
					} else {
						if got.Error != nil {
							t.Fatal(got.Error)
						}
						if request := <-seen; request.Completion != completion {
							t.Fatalf("provenance changed: %+v", request)
						}
					}
				case <-time.After(time.Second):
					t.Fatal("forward did not settle")
				}
			}
			if len(callHosts(t, alpha)) != 1 {
				t.Fatal("compatibility refusal broke federation")
			}
		})
	}
}
