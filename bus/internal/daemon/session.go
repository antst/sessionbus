// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"encoding/json"
	"syscall"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/structuredprocess"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const (
	maxPendingCalls     = 256
	supersedeWriteBound = time.Second
)

type answer struct {
	value  any
	code   int
	data   any
	remote *federation.Reply
}

type routedRequest struct {
	runID       string
	collect     bool
	destination *entry
	method      string
	params      any
	reply       chan answer
}

type replyEvent struct {
	requestID int64
	leg       int
	answer    answer
}

type supersedeEvent struct{}

type session struct {
	runID, runGeneration       string
	runSequence                uint64
	deadline, previousDeadline time.Time
	autoClose                  <-chan time.Time
	stopAuto                   func()

	daemon        *Daemon
	wire          *conn.Conn
	inbox         chan any
	identity      *entry
	lastIn        int64
	nextOut       int64
	pending       map[int64]routedRequest
	requests      map[int64]*requestState
	owned         int
	stopping      bool
	closed        bool
	child         *structuredprocess.Process
	launch        *launch
	committed     bool
	openID        int64
	closeCall     *routedRequest
	closeAnswer   answer
	closeTimer    *time.Timer
	forget        bool
	launchResult  *answer
	processExited bool
	stopSignal    syscall.Signal
	forwarded     chan federation.Reply
	caller        *federation.Caller
	omit          *entry
	messageID     string
}

func newSession(daemon *Daemon) *session {
	return &session{
		daemon:   daemon,
		inbox:    make(chan any, conn.OutboxSize),
		pending:  map[int64]routedRequest{},
		requests: map[int64]*requestState{},
	}
}

func (s *session) run() {
	shutdown := s.daemon.shutdown
	for {
		if s.closed && s.owned == 0 && (s.launch == nil || s.processExited) && s.drain() {
			if s.launch != nil {
				s.finishLane()
			}
			return
		}
		var closeTimer <-chan time.Time
		if s.closeTimer != nil {
			closeTimer = s.closeTimer.C
		}
		var spawnTimer <-chan time.Time
		if s.launch != nil && !s.committed {
			spawnTimer = s.launch.timer.C
		}
		select {
		case event := <-s.inbox:
			s.handleEvent(event)
		case <-s.autoClose:
			s.autoClose = nil
			s.policyClose()
		case <-closeTimer:
			s.closeTimer = nil
			s.hardStop()
		case <-spawnTimer:
			s.abortLaunch(answer{code: protocol.Timeout})
			s.hardStop()
		case <-shutdown:
			shutdown = nil
			if s.launch != nil && !s.committed {
				s.abortLaunch(answer{code: protocol.Internal, data: "daemon shutting down"})
			}
			s.orderlyStop()
		}
	}
}

func (s *session) handleEvent(event any) {
	switch value := event.(type) {
	case ownerEndEvent:
		s.ownerEnded(value.owner)
	case conn.Frame:
		if !s.stopping {
			s.handleFrame(value)
		}
	case conn.Closed:
		s.connectionClosed()
		if s.launch != nil {
			if !s.committed && s.launchResult == nil {
				s.abortLaunch(answer{code: protocol.SpawnFailed, data: failure(s.child, "worker exited before open")})
			}
			s.orderlyStop()
		}
	case routedRequest:
		if s.stopping || value.destination != s.identity || s.launch != nil && !s.committed {
			value.reply <- answer{code: protocol.NotConnected}
		} else {
			s.issue(value)
		}
	case replyEvent:
		s.consumeReply(value)
	case supersedeEvent:
		s.supersede()
	case processEvent:
		s.handleProcess(value)
	case spawnTimeout:
		if value.launch == s.launch && !s.committed {
			s.abortLaunch(answer{code: protocol.Timeout})
			s.hardStop()
		}
	}
}

func (s *session) handleFrame(event conn.Frame) {
	frame := event.Value
	if event.Err != nil {
		code := protocol.InvalidFrame
		if s.identity == nil {
			code = protocol.InvalidHello
		}
		s.reject(frame, code)
		return
	}
	if s.identity == nil {
		s.firstHello(frame)
		return
	}
	if !frame.Request {
		if s.launch != nil && frame.ID == s.openID {
			s.finishOpen(frame)
			return
		}
		s.receiveResponse(frame)
		return
	}
	if frame.ID <= s.lastIn {
		s.reject(frame, protocol.InvalidFrame)
		return
	}
	s.lastIn = frame.ID
	if frame.Method == "session.hello" && s.launch != nil {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	if s.launch != nil && !s.committed {
		s.error(frame, protocol.NotCommitted, nil)
		return
	}
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	if err != nil || !protocol.Allows(frame.Method, true) {
		code := protocol.InvalidFrame
		if frame.Method == "session.hello" {
			code = protocol.InvalidHello
		}
		s.reject(frame, code)
		return
	}
	if frame.Method == "session.hello" {
		hello, ok := params.(*protocol.PeerHello)
		if !ok {
			s.reject(frame, protocol.InvalidHello)
			return
		}
		s.peerHello(frame, hello)
		return
	}
	s.handleRequest(frame, params)
}

func (s *session) firstHello(frame protocol.Frame) {
	if !frame.Request || frame.ID <= s.lastIn || frame.Method != "session.hello" {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	s.lastIn = frame.ID
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	if err != nil {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	switch hello := params.(type) {
	case *protocol.PeerHello:
		s.peerHello(frame, hello)
	case *protocol.WorkerHello:
		start, ok := s.daemon.directory.claimWorker(s, hello.LaunchToken, hello.Product)
		if !ok {
			s.reject(frame, protocol.InvalidHello)
			return
		}
		start.claimed <- struct{}{}
		s.startLane(start, frame, hello.HelloDescription)
	default:
		s.reject(frame, protocol.InvalidHello)
	}
}

func (s *session) issue(request routedRequest) {
	if s.closeCall != nil && !request.collect {
		request.reply <- answer{code: protocol.Busy}
		return
	}
	if len(s.pending) >= maxPendingCalls {
		request.reply <- answer{code: protocol.Busy}
		return
	}
	method := request.method
	if method == "turn.start" || method == "turn.run" {
		request.method, request.collect = "turn.execute", method == "turn.run"
	}
	if code := s.daemon.directory.admit(s.identity, s, request.method); code != 0 {
		request.reply <- answer{code: code}
		return
	}
	if method == "session.close" {
		s.beginClose(request)
		return
	}
	if request.method == "turn.execute" {
		input := request.params.(*protocol.TurnRunRequest)
		request.runID = s.reserveRun()
		request.params = &protocol.ExecuteRequest{SessionID: s.identity.row.SessionID, RunID: request.runID, Input: input.Input}
	} else if method == "message.deliver" && !s.identity.peer && s.identity.row.Policy.IdleMessage == "run" && s.runID == "" {
		if s.closeCall != nil {
			request.reply <- answer{code: protocol.Busy}
			return
		}
		input := request.params.(protocol.DeliveryRequest)
		request.runID = s.reserveRun()
		input.RunID = request.runID
		request.params = input
	}
	// The worker sees its canonical opened identity even when the public caller
	// selected the lane by an unqualified local ID.
	switch input := request.params.(type) {
	case *protocol.ReadRequest:
		copy := *input
		copy.SessionID = s.identity.row.SessionID
		request.params = &copy
	case *protocol.WaitRequest:
		copy := *input
		copy.SessionID = s.identity.row.SessionID
		request.params = &copy
	case *protocol.RunRef:
		copy := *input
		copy.SessionID = s.identity.row.SessionID
		request.params = &copy
	}
	s.nextOut++
	body, err := protocol.RequestBytes(s.nextOut, request.method, request.params)
	if err != nil || !s.wire.Send(body) {
		if request.runID != "" {
			s.refuseRun(request.runID)
		}
		request.reply <- answer{code: protocol.NotConnected}
		return
	}
	if request.runID != "" {
		s.daemon.directory.admitted(s.identity, "turn.execute")
	}
	s.pending[s.nextOut] = request
}

func (s *session) receiveResponse(frame protocol.Frame) {
	request, ok := s.pending[frame.ID]
	if !ok {
		s.stopping = true
		s.wire.Close()
		return
	}
	delete(s.pending, frame.ID)
	if request.method == "turn.execute" && request.collect {
		defer s.writeDeferredClose()
	}
	if request.destination != s.identity || !s.daemon.directory.current(request.destination, s) {
		request.reply <- answer{code: protocol.NotConnected}
		return
	}

	if request.method == "session.close" {
		s.finishCloseResponse(frame)
		return
	}
	if frame.Error != nil {
		if request.runID != "" && (request.method == "turn.execute" || frame.Error.Code != protocol.Internal) {
			s.refuseRun(request.runID)
		}
		request.reply <- errorAnswer(frame.Error)
		return
	}
	value, err := protocol.DecodeResult(request.method, frame.Result)
	if err != nil {
		s.wire.Close()
		request.reply <- answer{code: protocol.NotConnected}
		return
	}
	if request.method == "turn.execute" {
		ref := value.(*protocol.RunRef)
		if ref.RunID != request.runID || ref.SessionID != s.identity.row.SessionID {
			s.wire.Close()
			request.reply <- answer{code: protocol.NotConnected}
			return
		}
		if request.collect {
			request.method, request.params, request.runID = "turn.wait", &protocol.WaitRequest{SessionID: ref.SessionID, RunID: ref.RunID}, ""
			s.issue(request)
			return
		}
	}
	request.reply <- answer{value: value}
}

func errorAnswer(value *protocol.RPCError) answer {
	result := answer{code: value.Code}
	if len(value.Data) != 0 {
		_ = json.Unmarshal(value.Data, &result.data)
	}
	return result
}

func (s *session) connectionClosed() {
	if s.closed {
		return
	}
	s.stopping, s.closed = true, true
	s.stopAutoClose()
	s.wire.OwnerClosed()
	s.daemon.directory.detach(s.identity, s)
	s.settlePending(protocol.NotConnected)
	for id := range s.requests {
		delete(s.requests, id)
	}
}

func (s *session) settlePending(code int) {
	for id, request := range s.pending {
		delete(s.pending, id)
		if request.method == "turn.run" {
			s.daemon.directory.finishRun(s.identity, s)
		}
		if s.closeCall != nil && request.reply == s.closeCall.reply {
			continue
		}
		request.reply <- answer{code: code}
	}
}

func (s *session) drain() bool {
	for {
		select {
		case event := <-s.inbox:
			s.handleEvent(event)
		default:
			return s.owned == 0
		}
	}
}

func (s *session) reject(frame protocol.Frame, code int) {
	if s.forwarded != nil {
		s.forwarded <- rpcReply(frame.Method, answer{code: code})
		return
	}
	s.stopping = true
	if frame.Request && frame.ID > 0 {
		body, _ := protocol.ErrorBytes(frame.ID, code, nil)
		if s.wire.Finish(body, supersedeWriteBound) {
			return
		}
	}
	s.wire.Close()
}

func (s *session) result(frame protocol.Frame, value any) {
	if s.forwarded != nil {
		s.forwarded <- rpcReply(frame.Method, answer{value: value})
		return
	}
	body, err := protocol.ResultBytes(frame.ID, frame.Method, value)
	if err != nil || !s.wire.Send(body) {
		s.wire.Close()
	}
}

func (s *session) error(frame protocol.Frame, code int, data any) {
	if s.forwarded != nil {
		s.forwarded <- rpcReply(frame.Method, answer{code: code, data: data})
		return
	}
	body, err := protocol.ErrorBytes(frame.ID, code, data)
	if err != nil || !s.wire.Send(body) {
		s.wire.Close()
	}
}

func (s *session) supersede() {
	if s.stopping {
		return
	}
	s.stopping = true
	s.detachRequests(protocol.Superseded)
	s.nextOut++
	body, err := protocol.RequestBytes(s.nextOut, "session.superseded", struct{}{})
	if err != nil || !s.wire.Finish(body, supersedeWriteBound) {
		s.wire.Close()
	}
}

func (s *session) await(requestID int64, leg int, reply <-chan answer, lifetime <-chan struct{}) {
	s.owned++
	go func() {
		value := answer{code: protocol.NotConnected}
		select {
		case value = <-reply:
		case <-lifetime:
		}
		s.inbox <- replyEvent{requestID: requestID, leg: leg, answer: value}
	}()
}
