// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"strings"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type commsRequest struct {
	from      *commslog.Endpoint
	to        *commslog.Endpoint
	messageID string
	runID     string
}

func endpoint(id, name, product string, groups []string) *commslog.Endpoint {
	host := ""
	if _, suffix, ok := strings.Cut(id, "@"); ok {
		host = suffix
	}
	return &commslog.Endpoint{SessionID: id, Host: host, Name: name, Product: product, Groups: append([]string(nil), groups...)}
}

func (d *Daemon) logEvent(event commslog.Event) {
	if d.comms != nil {
		d.comms.Emit(event)
	}
}

func (s *session) logCaller() *commslog.Endpoint {
	caller := s.federationCaller()
	return endpoint(caller.SessionID, caller.Name, caller.Product, caller.Groups)
}

func (s *session) logTarget(target string) *commslog.Endpoint {
	result := &commslog.Endpoint{Target: target}
	if _, host, ok := strings.Cut(target, "@"); ok {
		result.Host = host
	}
	s.daemon.directory.mu.Lock()
	if item := s.daemon.directory.entries[target]; item != nil {
		result = endpoint(item.row.SessionID, item.row.Name, item.row.Product, item.row.Groups)
		result.Target = target
	}
	s.daemon.directory.mu.Unlock()
	return result
}

// The caller supplies an identity already resolved by routing or a typed result.
func (s *session) logResolvedTarget(id string) *commslog.Endpoint {
	result := s.logTarget(id)
	result.SessionID = id
	return result
}

func (s *session) logRequest(frame protocol.Frame, params any, messageID string) {
	if s.daemon.comms == nil || s.omit != nil && s.messageID != "" {
		return
	}
	event := commslog.Event{Type: commslog.Request, Method: commslog.Method(frame.Method), From: s.logCaller(), To: &commslog.Endpoint{}}
	switch input := params.(type) {
	case *protocol.MessageSendRequest:
		event.MessageID = messageID
		event.To = &commslog.Endpoint{Target: input.Target, Host: input.Host}
		if input.Group != "" {
			event.To.Groups = []string{input.Group}
		}
		event.To.Targets = append([]string(nil), input.Targets...)
		event.Body = input.Message
	case *protocol.TurnRunRequest:
		event.To = s.logTarget(input.SessionID)
	case *protocol.ReadRequest:
		event.To = s.logTarget(input.SessionID)
		event.RunID = input.RunID
	case *protocol.WaitRequest:
		event.To = s.logTarget(input.SessionID)
		event.RunID = input.RunID
	case *protocol.RunRef:
		event.To = s.logTarget(input.SessionID)
		event.RunID = input.RunID
	case *protocol.SessionTarget:
		event.To = s.logTarget(input.SessionID)
	case *protocol.SessionCloseRequest:
		event.To = s.logTarget(input.SessionID)
	case *protocol.LaneSpawnRequest:
		event.To = &commslog.Endpoint{Host: input.Host, Target: input.ResumeSessionID, Name: input.Name, Product: input.Product}
	default:
		return
	}
	if s.commsRequests == nil {
		s.commsRequests = make(map[int64]commsRequest)
	}
	s.commsRequests[frame.ID] = commsRequest{from: event.From, to: event.To, messageID: messageID, runID: event.RunID}
	s.daemon.logEvent(event)
}

func (s *session) logResult(frame protocol.Frame, value any, code int) {
	captured, ok := s.commsRequests[frame.ID]
	if !ok {
		return
	}
	delete(s.commsRequests, frame.ID)
	event := commslog.Event{Type: commslog.Lifecycle, State: commslog.Settled, Method: commslog.Method(frame.Method), From: captured.from, To: captured.to, MessageID: captured.messageID, RunID: captured.runID}
	if code != 0 {
		event.State = commslog.Settled
		event.Outcome = commslog.Failed
		event.ErrorCode = code
		s.daemon.logEvent(event)
		return
	}
	switch result := value.(type) {
	case protocol.MessageSendResult:
		if len(result.Deliveries) == 0 {
			event.Outcome = commslog.Accepted
			s.daemon.logEvent(event)
			return
		}
		for _, delivery := range result.Deliveries {
			event.Type = commslog.Receipt
			event.MessageID = result.MessageID
			event.To = s.logResolvedTarget(delivery.SessionID)
			event.To.Target = delivery.Target
			if event.To.SessionID == "" {
				event.To.SessionID = delivery.SessionID
			}
			event.DeliveryID = delivery.DeliveryID
			event.Receipt = &commslog.DeliveryReceipt{DeliveryID: delivery.DeliveryID, Disposition: delivery.Disposition, Reason: delivery.Reason}
			s.daemon.logEvent(event)
		}
		return
	case *protocol.MessageSendResult:
		s.commsRequests[frame.ID] = captured
		s.logResult(frame, *result, 0)
		return
	case *protocol.RunRef:
		event.RunID = result.RunID
		event.To = s.logResolvedTarget(result.SessionID)
	case protocol.RunRef:
		event.RunID = result.RunID
		event.To = s.logResolvedTarget(result.SessionID)
	case *protocol.RunStatus:
		event.RunID = result.RunID
		event.State = commslog.Running
		if result.State != "running" {
			event.State = commslog.Settled
		}
		if result.State == "unavailable" {
			event.Outcome = commslog.Unavailable
		}
		if result.Result != nil {
			event.Outcome = commslog.Outcome(result.Result.Outcome)
		}
	case *protocol.LaneSpawnResult:
		event.To = s.logResolvedTarget(result.SessionID)
	case protocol.LaneSpawnResult:
		event.To = s.logResolvedTarget(result.SessionID)
	}
	s.daemon.logEvent(event)
}

// logDispatch records admission to the recipient owner queue, not native
// consumption. The send header is the only message event carrying its body.
func (s *session) logDispatch(messageID, deliveryID, target, recipient string) {
	if s.daemon.comms == nil {
		return
	}
	to := s.logResolvedTarget(recipient)
	to.Target = target
	s.daemon.logEvent(commslog.Event{Type: commslog.Request, Method: commslog.MessageDeliver,
		MessageID: messageID, DeliveryID: deliveryID, From: s.logCaller(), To: to, Outcome: commslog.Accepted})
}

func (s *session) logSession(state commslog.State) {
	if s.daemon.comms == nil || s.identity == nil {
		return
	}
	s.daemon.logEvent(commslog.Event{Type: commslog.Lifecycle, Method: commslog.Method("session.connection"),
		From: s.logCaller(), State: state})
}

func (s *session) logRun(method commslog.Method, runID string, state commslog.State, outcome string) {
	if s.daemon.comms == nil {
		return
	}
	s.daemon.logEvent(commslog.Event{Type: commslog.Lifecycle, Method: method, From: s.logCaller(),
		RunID: runID, State: state, Outcome: commslog.Outcome(outcome)})
}
