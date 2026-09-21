// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestEndedRunDeliveryIsRefusedBeforeNativeSubmission(t *testing.T) {
	ready := make(chan *rpc.Request, 1)
	var delivered atomic.Int32
	p := &cursorProduct{
		ready: func(request *rpc.Request, _ *rpc.Conn) { ready <- request },
		deliver: func(context.Context, DeliveryRequest, *Run) (DeliveryReceipt, error) {
			delivered.Add(1)
			return DeliveryReceipt{Disposition: "written"}, nil
		},
	}
	_, wire := cursorWire(t, p)
	cursorOpen(t, wire)
	if err := cursorExecute(wire, "native@local", "g/1"); err != nil {
		t.Fatal(err)
	}
	request := <-ready
	message := DeliveryRequest{MessageID: "late", From: DeliverySource{SessionID: "peer@local", Product: "peer", Groups: []string{}}, Body: "work"}
	var receipt DeliveryReceipt
	cursorCode(t, wire.Call(context.Background(), "message.deliver", message, &receipt), protocol.NotRunning)
	if err := wire.Result(request, struct{}{}); err != nil {
		t.Fatal(err)
	}
	cursorWait(t, wire, "g/1")
	cursorCode(t, wire.Call(context.Background(), "message.deliver", message, &receipt), protocol.NotRunning)
	if delivered.Load() != 0 {
		t.Fatal("ended run reached product delivery")
	}
}

func TestNativeHandoffRefusalRemainsNotRunning(t *testing.T) {
	release := make(chan struct{})
	p := &cursorProduct{
		run: func(context.Context, *Run, RunInput) (TurnResult, error) {
			<-release
			return TurnResult{Outcome: "completed", Result: "done"}, nil
		},
		deliver: func(context.Context, DeliveryRequest, *Run) (DeliveryReceipt, error) {
			return DeliveryReceipt{}, &ProtocolError{Code: protocol.NotRunning, Message: "not_running"}
		},
	}
	_, wire := cursorWire(t, p)
	t.Cleanup(func() { close(release) })
	cursorOpen(t, wire)
	if err := cursorExecute(wire, "native@local", "g/1"); err != nil {
		t.Fatal(err)
	}
	message := DeliveryRequest{MessageID: "crossing", From: DeliverySource{SessionID: "peer@local", Product: "peer", Groups: []string{}}, Body: "work"}
	cursorCode(t, wire.Call(context.Background(), "message.deliver", message, &DeliveryReceipt{}), protocol.NotRunning)
}
