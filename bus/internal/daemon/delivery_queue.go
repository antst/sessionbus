// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"strconv"
	"strings"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Every acknowledged queue entry reserves a future retained worker record.
// The current run already counts in runSequence. A successful in-order ack is
// the only operation that releases admitted records; terminal completion does
// not. Thus an explicit start cannot consume capacity promised to queued work.
func (s *session) hasRunCapacity() bool {
	return s.runSequence-s.acknowledgedSequence+uint64(len(s.deferredDeliveries)) < protocol.MaxOperations
}

func (s *session) recordAcknowledgment(ref *protocol.RunRef) {
	generation, suffix, ok := strings.Cut(ref.RunID, "/")
	sequence, err := strconv.ParseUint(suffix, 10, 64)
	if ok && err == nil && generation == s.runGeneration && ref.SessionID == s.identity.row.SessionID && sequence <= s.runSequence && sequence > s.acknowledgedSequence {
		s.acknowledgedSequence = sequence
	}
}

// Drain FIFO one run at a time, after the preceding ready acknowledgment. Other
// entries keep their reservations and are never retried as ordinary deliveries.
func (s *session) startDeferredDelivery() {
	for s.runID == "" && len(s.deferredDeliveries) > 0 {
		request := s.deferredDeliveries[0]
		s.deferredDeliveries[0] = routedRequest{}
		s.deferredDeliveries = s.deferredDeliveries[1:]
		s.issue(request)
	}
}

// An automatically scheduled delivery has already answered its caller exactly
// once. Its later native admission, failure, or loss belongs in the recipient's
// communication log, not in a second sender receipt. No message body is logged.
func (s *session) answerRouted(request routedRequest, result answer) {
	if !request.scheduled {
		request.reply <- result
		return
	}
	input := request.params.(protocol.DeliveryRequest)
	event := commslog.Event{Type: commslog.Lifecycle, Method: commslog.MessageDeliver,
		MessageID: input.MessageID, RunID: request.runID, State: commslog.Settled,
		From: endpoint(input.From.SessionID, input.From.Name, input.From.Product, input.From.Groups),
		To:   s.logCaller(), ErrorCode: result.code}
	if receipt, ok := result.value.(*protocol.DeliveryReceipt); ok {
		event.Type = commslog.Receipt
		event.Receipt = &commslog.DeliveryReceipt{Disposition: receipt.Disposition, Reason: receipt.Reason}
	} else {
		event.Outcome = commslog.NotSubmitted
		if request.runID != "" {
			event.Outcome = commslog.NoReceipt
		}
	}
	s.daemon.logEvent(event)
}
