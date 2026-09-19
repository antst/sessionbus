// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const (
	maxTraceBytes        = 4 << 20
	maxTraceWork         = 256
	maxTraceRecipients   = 256
	traceDeliveryTimeout = 5 * time.Second
)

// These objects belong only to a live send or attachment. None is a table row.
type traceRequest struct {
	messageID string
	from      protocol.DeliverySource
	input     protocol.MessageSendRequest
	refs      []federation.TraceRecipient
	bytes     int
}

// traceEnvelope is the body of an ordinary daemon-generated message. It never
// carries the private routing/lifetime fields in federation.TraceRecipient.
type traceEnvelope struct {
	Kind            string                         `json:"kind"`
	MessageID       string                         `json:"message_id"`
	From            protocol.DeliverySource        `json:"from"`
	Targets         []string                       `json:"targets,omitempty"`
	Group           string                         `json:"group,omitempty"`
	Host            string                         `json:"host,omitempty"`
	MatchedChildren []string                       `json:"matched_children"`
	Body            *string                        `json:"body,omitempty"`
	Deliveries      []protocol.MessageSendDelivery `json:"deliveries"`
	ErrorCode       int                            `json:"error_code,omitempty"`
}

func traceMode(mode string) bool { return mode == "events" || mode == "content" }

func (s *session) configureTrace(frame protocol.Frame, input *protocol.TraceConfigureRequest) {
	id, host, code := canonicalTarget(input.SessionID, s.daemon.host, validIDPart)
	if code != 0 {
		s.error(frame, code, nil)
		return
	}
	if host != s.daemon.host {
		s.forward(frame, id)
		return
	}
	owner := s.daemon.directory.callerOwner(s)
	d := s.daemon.directory
	d.mu.Lock()
	item := d.entries[id]
	code = 0
	switch {
	case d.closing || owner == nil || owner.ended:
		code = protocol.NotConnected
	case item == nil || item.peer || item.parent != owner:
		code = protocol.UnknownSession
	case item.claimed:
		code = protocol.Busy
	default:
		item.traceMode, item.traceVersion = input.Mode, randomID("policy")
	}
	d.mu.Unlock()
	if code != 0 {
		s.error(frame, code, nil)
		return
	}
	s.result(frame, protocol.TraceConfigureResult{SessionID: id, Mode: input.Mode})
}

// snapshotTrace runs under directory.mu and only emits a live, authoritative
// parent route. The caller's supplied session ID is never sufficient authority.
func (d *directory) snapshotTrace(item *entry) (federation.TraceRecipient, bool) {
	if item == nil || item.peer || !traceMode(item.traceMode) || item.parent == nil || item.parent.ended {
		return federation.TraceRecipient{}, false
	}
	owner := item.parent
	caller := owner.caller
	if owner.host == "" {
		parent := d.entries[owner.id]
		if parent == nil || parent.lifetime != owner || parent.attachment == nil {
			return federation.TraceRecipient{}, false
		}
		caller = federation.Caller{SessionID: owner.id, OwnerLifetime: owner.token,
			Name: parent.row.Name, Product: parent.row.Product, PrivateGroup: privateGroup(parent),
			Groups: append([]string(nil), parent.row.Groups...)}
	} else {
		caller.Groups = append([]string(nil), caller.Groups...)
	}
	return federation.TraceRecipient{Child: item.row.SessionID, Mode: item.traceMode, Version: item.traceVersion, Owner: caller}, true
}

func (d *Daemon) reserveTrace(size int) bool {
	d.directory.mu.Lock()
	defer d.directory.mu.Unlock()
	if d.directory.closing || size > maxTraceBytes-d.traceBytes || d.traceCount >= maxTraceWork {
		d.traceDropped++
		return false
	}
	d.traceCount++
	d.traceBytes += size
	return true
}

func (d *Daemon) releaseTrace(size int) {
	d.directory.mu.Lock()
	d.traceCount--
	d.traceBytes -= size
	d.directory.mu.Unlock()
}

func (d *Daemon) dropTrace() {
	d.directory.mu.Lock()
	d.traceDropped++
	d.directory.mu.Unlock()
}

func (s *session) beginTrace(frame protocol.Frame, params any, messageID string) {
	input, ok := params.(*protocol.MessageSendRequest)
	if !ok || s.traceCopy != nil || s.forwarded != nil && (s.messageID != "" || !s.traceCollect) {
		return
	}
	// Reserve the original projection once. Remote targets may supply eligibility
	// only with their existing reply, so retain it within this bounded operation.
	from := s.federationCaller()
	captured := &traceRequest{messageID: messageID,
		from: protocol.DeliverySource{SessionID: from.SessionID, Name: from.Name, Product: from.Product, Groups: append([]string{}, from.Groups...)}, input: *input}
	captured.input.Targets = append([]string(nil), input.Targets...)
	raw, err := json.Marshal(captured.input)
	if err != nil {
		return
	}
	captured.bytes = len(raw) + len(from.SessionID) + len(from.Name) + 4096
	if !s.daemon.reserveTrace(captured.bytes) {
		return
	}
	if s.traceRequests == nil {
		s.traceRequests = map[int64]*traceRequest{}
	}
	s.traceRequests[frame.ID] = captured
	s.daemon.directory.mu.Lock()
	ref, enabled := s.daemon.directory.snapshotTrace(s.daemon.directory.entries[from.SessionID])
	s.daemon.directory.mu.Unlock()
	if enabled {
		s.addTraceRef(frame.ID, ref)
	}
}

func (s *session) traceTarget(id int64, item *entry, target string) {
	if s.traceCopy != nil || s.traceRequests[id] == nil && !s.traceCollect {
		return
	}
	s.daemon.directory.mu.Lock()
	ref, enabled := s.daemon.directory.snapshotTrace(item)
	s.daemon.directory.mu.Unlock()
	if enabled {
		ref.Target = target
		s.addTraceRef(id, ref)
	}
}

func (s *session) addTraceRef(id int64, ref federation.TraceRecipient) {
	refs := &s.traceTargets
	if captured := s.traceRequests[id]; captured != nil {
		refs = &captured.refs
	}
	for _, previous := range *refs {
		if previous.Child == ref.Child {
			return
		}
	}
	if len(*refs) >= maxTraceRecipients {
		s.daemon.dropTrace()
		return
	}
	// Bound the eligibility projection independently of the original body. The
	// actual queue reservation is conservative and includes its encoded bytes.
	raw, err := json.Marshal(ref)
	if err != nil || len(raw) > 16384 {
		s.daemon.dropTrace()
		return
	}
	if s.traceRequests[id] == nil {
		if s.traceTargetsBytes+len(raw) > 65536 {
			s.daemon.dropTrace()
			return
		}
		if s.traceTargetsBytes == 0 {
			if !s.daemon.reserveTrace(len(raw)) {
				return
			}
		} else {
			s.daemon.directory.mu.Lock()
			if len(raw) > maxTraceBytes-s.daemon.traceBytes {
				s.daemon.traceDropped++
				s.daemon.directory.mu.Unlock()
				return
			}
			s.daemon.traceBytes += len(raw)
			s.daemon.directory.mu.Unlock()
		}
		s.traceTargetsBytes += len(raw)
	}
	if captured := s.traceRequests[id]; captured != nil {
		s.daemon.directory.mu.Lock()
		if len(raw) > maxTraceBytes-s.daemon.traceBytes {
			s.daemon.traceDropped++
			s.daemon.directory.mu.Unlock()
			return
		}
		s.daemon.traceBytes += len(raw)
		captured.bytes += len(raw)
		s.daemon.directory.mu.Unlock()
	}
	*refs = append(*refs, ref)
}

func (s *session) releaseTraceTargets() {
	if s.traceTargetsBytes != 0 {
		s.daemon.releaseTrace(s.traceTargetsBytes)
		s.traceTargetsBytes = 0
	}
}

func (s *session) traceReply(method string, value answer) federation.Reply {
	defer s.releaseTraceTargets()
	reply := rpcReply(method, value)
	if s.traceCollect && s.traceCopy == nil && method == "message.send" {
		reply.Trace = append([]federation.TraceRecipient{}, s.traceTargets...)
	}
	return reply
}

func (s *session) validTraceCopy(input *protocol.MessageSendRequest) bool {
	if s.traceCopy == nil {
		return true
	}
	// Actual lifetime admission is also checked under directory.mu by route,
	// closing the replacement race between target selection and dispatch.
	return input.Target == s.traceCopy.SessionID && len(input.Targets) == 0 && input.Group == "" && s.traceCopy.Lifetime != ""
}

func (s *session) finishTrace(frame protocol.Frame, result any, code int, extra []federation.TraceRecipient) {
	captured := s.traceRequests[frame.ID]
	if captured == nil {
		return
	}
	for _, ref := range extra {
		s.addTraceRef(frame.ID, ref)
	}
	delete(s.traceRequests, frame.ID)
	defer s.daemon.releaseTrace(captured.bytes)
	var deliveries []protocol.MessageSendDelivery
	switch value := result.(type) {
	case protocol.MessageSendResult:
		deliveries = value.Deliveries
	case *protocol.MessageSendResult:
		deliveries = value.Deliveries
	}
	byParent := map[string][]federation.TraceRecipient{}
	for _, ref := range captured.refs {
		if !traceMode(ref.Mode) || ref.Owner.SessionID == "" || ref.Owner.OwnerLifetime == "" {
			continue
		}
		// Local policy changes can discard pending observations. Remote policy is
		// the destination's admission snapshot, never upgraded at settlement.
		if strings.HasSuffix(ref.Child, "@"+s.daemon.host) {
			s.daemon.directory.mu.Lock()
			current, ok := s.daemon.directory.snapshotTrace(s.daemon.directory.entries[ref.Child])
			s.daemon.directory.mu.Unlock()
			if !ok || current.Version != ref.Version || current.Owner.OwnerLifetime != ref.Owner.OwnerLifetime {
				continue
			}
		}
		key := ref.Owner.SessionID + "\x00" + ref.Owner.OwnerLifetime
		byParent[key] = append(byParent[key], ref)
	}
	for _, refs := range byParent {
		// The original already went only to this parent. Check the complete
		// settled aggregate, before projecting it to this parent's children;
		// unresolved targets and fanout must still produce useful copies.
		if len(deliveries) == 1 && deliveries[0].SessionID == refs[0].Owner.SessionID {
			continue
		}
		body := traceEnvelope{Kind: "sessionbus.trace", MessageID: captured.messageID, From: captured.from, Deliveries: []protocol.MessageSendDelivery{}, ErrorCode: code}
		children := map[string]bool{}
		source := false
		for _, ref := range refs {
			if !children[ref.Child] {
				body.MatchedChildren = append(body.MatchedChildren, ref.Child)
			}
			children[ref.Child] = true
			source = source || ref.Child == captured.from.SessionID
			if ref.Mode == "content" {
				body.Body = &captured.input.Message
			}
		}
		sort.Strings(body.MatchedChildren)
		if source {
			body.Targets = append([]string(nil), captured.input.Targets...)
			if captured.input.Target != "" {
				body.Targets = []string{captured.input.Target}
			}
			body.Group, body.Host = captured.input.Group, captured.input.Host
		}
		for _, delivery := range deliveries {
			if delivery.SessionID == "" {
				for _, ref := range refs {
					if ref.Target != "" && ref.Target == delivery.Target {
						delivery.SessionID = ref.Child
						break
					}
				}
			}
			if source || children[delivery.SessionID] {
				body.Deliveries = append(body.Deliveries, delivery)
			}
		}
		raw, err := json.Marshal(body)
		if err != nil || utf8.RuneCount(raw) > protocol.MaxTextRunes {
			s.daemon.dropTrace()
			continue
		}
		s.daemon.enqueueTrace(refs[0].Owner, string(raw))
	}
}

func (d *Daemon) enqueueTrace(parent federation.Caller, body string) {
	size := len(body) + 4096
	if !d.reserveTrace(size) {
		return
	}
	d.directory.mu.Lock()
	if d.directory.closing {
		d.directory.mu.Unlock()
		d.releaseTrace(size)
		return
	}
	d.group.Add(1) // Admission and Add precede Close's Wait under this mutex.
	d.directory.mu.Unlock()
	go func() {
		defer d.group.Done()
		defer d.releaseTrace(size)
		ctx, cancel := context.WithTimeout(d.traceContext, traceDeliveryTimeout)
		defer cancel()
		if ctx.Err() != nil {
			return
		}
		caller := federation.Caller{SessionID: d.traceSource, Name: "Sessionbus trace@" + d.host,
			Product: "sessionbus", PrivateGroup: parent.PrivateGroup, Groups: []string{parent.PrivateGroup}}
		copy := &federation.TraceDestination{SessionID: parent.SessionID, Lifetime: parent.OwnerLifetime}
		wait := d.directory.forwardLocalTrace(caller, nil, "message.send", &protocol.MessageSendRequest{Target: parent.SessionID, Message: body}, "", false, copy)
		reply, ok := wait(ctx.Done())
		if !ok || reply.Error != nil {
			d.dropTrace()
			return
		}
		var result protocol.MessageSendResult
		if protocol.UnmarshalResult("message.send", reply.Result, &result) != nil || len(result.Deliveries) != 1 || !slices.Contains([]string{"written", "injected", "queued_for_next_turn"}, result.Deliveries[0].Disposition) {
			d.dropTrace()
		}
	}()
}
