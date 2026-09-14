// SPDX-License-Identifier: GPL-3.0-only

// Package commslog writes the optional operator communication log. It accepts
// typed, redacted events only; protocol frames and arbitrary JSON never enter
// this package.
package commslog

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const Schema = "sessionbus.comms.v1"

const (
	minimumFileBytes  = 4096
	minimumQueueBytes = 4096
	maximumLabelBytes = 1024
	maximumFilters    = 4096
	maximumGroups     = 4096
)

type Mode string

const (
	Off      Mode = "off"
	Metadata Mode = "metadata"
	Content  Mode = "content"
)

type EventType string

const (
	Lifecycle EventType = "lifecycle"
	Request   EventType = "request"
	Receipt   EventType = "receipt"
)

type Method string

const (
	DaemonStart        Method = "daemon.start"
	DaemonStop         Method = "daemon.stop"
	MessageSend        Method = "message.send"
	MessageDeliver     Method = "message.deliver"
	MessageReceipt     Method = "message.receipt"
	TurnStart          Method = "turn.start"
	TurnStatus         Method = "turn.status"
	TurnExecute        Method = "turn.execute"
	TurnWait           Method = "turn.wait"
	TurnResult         Method = "turn.result"
	TurnReady          Method = "turn.ready"
	TurnAck            Method = "turn.ack"
	SessionClose       Method = "session.close"
	LaneSpawn          Method = "lane.spawn"
	LaneOpen           Method = "lane.open"
	LaneClose          Method = "lane.close"
	LaneForget         Method = "lane.forget"
	ConnectionClosed   Method = "connection.closed"
	FederationForward  Method = "federation.forward"
	FederationResponse Method = "federation.response"
)

type State string

const (
	Starting     State = "starting"
	Ready        State = "ready"
	Connected    State = "connected"
	Running      State = "running"
	Idle         State = "idle"
	Settled      State = "settled"
	Closing      State = "closing"
	Closed       State = "closed"
	Disconnected State = "disconnected"
	Superseded   State = "superseded"
)

type Outcome string

const (
	Accepted     Outcome = "accepted"
	Rejected     Outcome = "rejected"
	Completed    Outcome = "completed"
	Failed       Outcome = "failed"
	Canceled     Outcome = "canceled"
	Unavailable  Outcome = "unavailable"
	NoAgent      Outcome = "no_agent"
	Interrupted  Outcome = "interrupted"
	NoReceipt    Outcome = "no_receipt"
	NotSubmitted Outcome = "not_submitted"
)

type Endpoint struct {
	SessionID string   `json:"session_id,omitempty"`
	Host      string   `json:"host,omitempty"`
	Target    string   `json:"target,omitempty"`
	Targets   []string `json:"targets,omitempty"`
	Product   string   `json:"product,omitempty"`
	Name      string   `json:"name,omitempty"`
	Groups    []string `json:"groups,omitempty"`
}

type DeliveryReceipt struct {
	DeliveryID  string `json:"delivery_id,omitempty"`
	Disposition string `json:"disposition"`
	Reason      string `json:"reason,omitempty"`
}

const (
	ReceiptWritten           = "written"
	ReceiptInjected          = "injected"
	ReceiptQueuedForNextTurn = "queued_for_next_turn"
	ReceiptRejected          = "rejected"
)

type Event struct {
	Type       EventType        `json:"type"`
	Method     Method           `json:"method"`
	MessageID  string           `json:"message_id,omitempty"`
	DeliveryID string           `json:"delivery_id,omitempty"`
	RunID      string           `json:"run_id,omitempty"`
	State      State            `json:"state,omitempty"`
	Outcome    Outcome          `json:"outcome,omitempty"`
	From       *Endpoint        `json:"from,omitempty"`
	To         *Endpoint        `json:"to,omitempty"`
	Receipt    *DeliveryReceipt `json:"receipt,omitempty"`
	Body       string           `json:"body,omitempty"`
}

type Options struct {
	Mode         Mode
	Path         string
	Host         string
	Incarnation  string
	MaxFileBytes int64
	MaxFiles     int
	QueueBytes   int
	Sessions     []string
	Groups       []string
	// OnError is invoked outside the queue mutex at most once. It is intended
	// for a small diagnostic write and must not call back into Logger.
	OnError func(error)
}

type Stats struct {
	LastSequence uint64
	Written      uint64
	GapRecords   uint64
	LostInvalid  uint64
	LostOverflow uint64
	LostWrite    uint64
	WriteErrors  uint64
}

type diskRecord struct {
	Schema      string           `json:"schema"`
	Host        string           `json:"host"`
	Incarnation string           `json:"incarnation"`
	Sequence    uint64           `json:"seq"`
	UTC         time.Time        `json:"utc"`
	Type        EventType        `json:"type"`
	Method      Method           `json:"method,omitempty"`
	MessageID   string           `json:"message_id,omitempty"`
	DeliveryID  string           `json:"delivery_id,omitempty"`
	RunID       string           `json:"run_id,omitempty"`
	State       State            `json:"state,omitempty"`
	Outcome     Outcome          `json:"outcome,omitempty"`
	From        *Endpoint        `json:"from,omitempty"`
	To          *Endpoint        `json:"to,omitempty"`
	Receipt     *DeliveryReceipt `json:"receipt,omitempty"`
	Body        string           `json:"body,omitempty"`
	Gap         *gapRecord       `json:"gap,omitempty"`
}

type gapRecord struct {
	FirstSequence uint64 `json:"first_seq"`
	LastSequence  uint64 `json:"last_seq"`
	Count         uint64 `json:"count"`
	Cause         string `json:"cause"`
}

type pendingGap struct {
	first uint64
	last  uint64
	count uint64
	at    time.Time
}

type queuedRecord struct {
	raw    []byte
	events uint64
	isGap  bool
}

type recordSink interface {
	WriteRecord([]byte) error
	Close() error
}

type Logger struct {
	mode          Mode
	host          string
	incarnation   string
	maxFileBytes  int64
	maxQueueBytes int
	sessions      map[string]struct{}
	groups        map[string]struct{}
	onError       func(error)
	now           func() time.Time
	sink          recordSink

	mu         sync.Mutex
	notify     chan struct{}
	done       chan struct{}
	queue      []queuedRecord
	queueBytes int
	gap        *pendingGap
	closing    bool
	err        error
	stats      Stats
}

func Open(options Options) (*Logger, error) {
	if options.Mode == Off {
		return newOffLogger(), nil
	}
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	sink, err := openRotatingFile(options.Path, options.MaxFileBytes, options.MaxFiles)
	if err != nil {
		return nil, err
	}
	return newLogger(options, sink), nil
}

func newOffLogger() *Logger {
	done := make(chan struct{})
	close(done)
	return &Logger{mode: Off, done: done}
}

func newLogger(options Options, sink recordSink) *Logger {
	logger := &Logger{
		mode:          options.Mode,
		host:          options.Host,
		incarnation:   options.Incarnation,
		maxFileBytes:  options.MaxFileBytes,
		maxQueueBytes: options.QueueBytes,
		sessions:      stringSet(options.Sessions),
		groups:        stringSet(options.Groups),
		onError:       options.OnError,
		now:           time.Now,
		sink:          sink,
		notify:        make(chan struct{}, 1),
		done:          make(chan struct{}),
	}
	go logger.writeLoop()
	return logger
}

func validateOptions(options Options) error {
	if options.Mode != Metadata && options.Mode != Content {
		return fmt.Errorf("invalid communication log mode %q", options.Mode)
	}
	if options.Path == "" || options.Host == "" || options.Incarnation == "" {
		return errors.New("communication log path, host, and incarnation are required")
	}
	if len(options.Host) > maximumLabelBytes || len(options.Incarnation) > maximumLabelBytes {
		return errors.New("communication log host or incarnation is too long")
	}
	if options.MaxFileBytes < minimumFileBytes || options.QueueBytes < minimumQueueBytes || options.MaxFiles < 1 {
		return errors.New("invalid communication log bounds")
	}
	if !validFilters(options.Sessions) || !validFilters(options.Groups) {
		return errors.New("invalid communication log filter")
	}
	return nil
}

func validFilters(values []string) bool {
	if len(values) > maximumFilters {
		return false
	}
	for _, value := range values {
		if value == "" || len(value) > maximumLabelBytes {
			return false
		}
	}
	return true
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	return set
}

// Emit admits an event without waiting for filesystem I/O. It returns false
// when logging is disabled, the event is filtered or invalid, the byte queue is
// full, or the logger has begun closing.
func (l *Logger) Emit(event Event) bool {
	if l == nil || (l.mode != Metadata && l.mode != Content) {
		return false
	}
	if !l.matches(event) {
		return false
	}
	if !validEndpoint(event.From) || !validEndpoint(event.To) {
		l.recordInvalid()
		return false
	}
	event = cloneEvent(event)
	if l.mode != Content {
		event.Body = ""
	}
	if !validEvent(event) {
		l.recordInvalid()
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing || l.err != nil {
		return false
	}
	l.stats.LastSequence++
	sequence := l.stats.LastSequence
	at := l.now().UTC()
	raw, err := marshalLine(diskRecord{
		Schema: Schema, Host: l.host, Incarnation: l.incarnation,
		Sequence: sequence, UTC: at, Type: event.Type, Method: event.Method,
		MessageID: event.MessageID, DeliveryID: event.DeliveryID,
		RunID: event.RunID, State: event.State,
		Outcome: event.Outcome, From: event.From,
		To: event.To, Receipt: event.Receipt, Body: event.Body,
	})
	if err != nil {
		l.addGap(sequence, at)
		l.stats.LostOverflow++
		return false
	}
	if len(raw) > l.maxQueueBytes || int64(len(raw)) > l.maxFileBytes {
		l.addGap(sequence, at)
		l.stats.LostOverflow++
		return false
	}

	var gap queuedRecord
	if l.gap != nil {
		gap = l.gapLine()
	}
	needed := len(raw) + len(gap.raw)
	if l.queueBytes+needed > l.maxQueueBytes || int64(len(gap.raw)) > l.maxFileBytes {
		l.addGap(sequence, at)
		l.stats.LostOverflow++
		return false
	}
	if gap.raw != nil {
		l.queue = append(l.queue, gap)
		l.queueBytes += len(gap.raw)
		l.gap = nil
	}
	l.queue = append(l.queue, queuedRecord{raw: raw, events: 1})
	l.queueBytes += len(raw)
	l.wake()
	return true
}

func (l *Logger) recordInvalid() {
	l.mu.Lock()
	if !l.closing && l.err == nil {
		l.stats.LostInvalid++
	}
	l.mu.Unlock()
}

func cloneEvent(event Event) Event {
	if event.From != nil {
		value := *event.From
		value.Groups = append([]string(nil), value.Groups...)
		value.Targets = append([]string(nil), value.Targets...)
		event.From = &value
	}
	if event.To != nil {
		value := *event.To
		value.Groups = append([]string(nil), value.Groups...)
		value.Targets = append([]string(nil), value.Targets...)
		event.To = &value
	}
	if event.Receipt != nil {
		value := *event.Receipt
		event.Receipt = &value
	}
	return event
}

func validEvent(event Event) bool {
	if !validMethod(event.Method) {
		return false
	}
	if !validState(event.State) || !validOutcome(event.Outcome) {
		return false
	}
	if !validEndpoint(event.From) || !validEndpoint(event.To) {
		return false
	}
	if event.Body != "" && (event.Type != Request || event.Method != MessageSend) {
		return false
	}
	switch event.Type {
	case Lifecycle, Request:
		return event.Receipt == nil
	case Receipt:
		return validReceipt(event.Receipt)
	default:
		return false
	}
}

func validReceipt(receipt *DeliveryReceipt) bool {
	if receipt == nil {
		return false
	}
	switch receipt.Disposition {
	case ReceiptWritten, ReceiptInjected, ReceiptQueuedForNextTurn:
		return receipt.Reason == ""
	case ReceiptRejected:
		return receipt.Reason != ""
	default:
		return false
	}
}

func validEndpoint(endpoint *Endpoint) bool {
	if endpoint == nil {
		return true
	}
	if len(endpoint.Targets) > 256 || len(endpoint.Groups) > maximumGroups {
		return false
	}
	for _, target := range endpoint.Targets {
		if target == "" {
			return false
		}
	}
	return true
}

func validMethod(method Method) bool {
	return method != "" && len(method) <= maximumLabelBytes
}

func validState(state State) bool {
	switch state {
	case "", Starting, Ready, Connected, Running, Idle, Settled, Closing, Closed, Disconnected, Superseded:
		return true
	default:
		return false
	}
}

func validOutcome(outcome Outcome) bool {
	switch outcome {
	case "", Accepted, Rejected, Completed, Failed, Canceled, Unavailable, NoAgent, Interrupted, NoReceipt, NotSubmitted:
		return true
	default:
		return false
	}
}

func (l *Logger) matches(event Event) bool {
	if len(l.sessions) == 0 && len(l.groups) == 0 {
		return true
	}
	return l.matchesEndpoint(event.From) || l.matchesEndpoint(event.To)
}

func (l *Logger) matchesEndpoint(endpoint *Endpoint) bool {
	if endpoint == nil {
		return false
	}
	if _, ok := l.sessions[endpoint.SessionID]; ok {
		return true
	}
	for _, group := range endpoint.Groups {
		if _, ok := l.groups[group]; ok {
			return true
		}
	}
	return false
}

func marshalLine(record diskRecord) ([]byte, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func (l *Logger) addGap(sequence uint64, at time.Time) {
	if l.gap == nil {
		l.gap = &pendingGap{first: sequence, last: sequence, count: 1, at: at}
		return
	}
	l.gap.last = sequence
	l.gap.count++
}

func (l *Logger) gapLine() queuedRecord {
	raw, _ := marshalLine(diskRecord{
		Schema: Schema, Host: l.host, Incarnation: l.incarnation,
		Sequence: l.gap.last, UTC: l.gap.at, Type: EventType("gap"),
		Gap: &gapRecord{FirstSequence: l.gap.first, LastSequence: l.gap.last,
			Count: l.gap.count, Cause: "queue_overflow"},
	})
	return queuedRecord{raw: raw, isGap: true}
}

func (l *Logger) wake() {
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

func (l *Logger) writeLoop() {
	defer close(l.done)
	for {
		record, done := l.take()
		if done {
			if err := l.sink.Close(); err != nil {
				l.fail(err, 0)
			}
			return
		}
		if record.raw == nil {
			<-l.notify
			continue
		}
		if err := l.sink.WriteRecord(record.raw); err != nil {
			closeErr := l.sink.Close()
			l.fail(errors.Join(err, closeErr), record.events)
			return
		}
		l.mu.Lock()
		if record.isGap {
			l.stats.GapRecords++
		} else {
			l.stats.Written += record.events
		}
		l.queueBytes -= len(record.raw)
		l.mu.Unlock()
	}
}

func (l *Logger) take() (queuedRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.queue) != 0 {
		record := l.queue[0]
		l.queue[0] = queuedRecord{}
		l.queue = l.queue[1:]
		return record, false
	}
	if !l.closing {
		return queuedRecord{}, false
	}
	if l.gap != nil {
		record := l.gapLine()
		l.gap = nil
		return record, false
	}
	return queuedRecord{}, true
}

func (l *Logger) fail(err error, current uint64) {
	if err == nil {
		return
	}
	l.mu.Lock()
	if l.err != nil {
		l.mu.Unlock()
		return
	}
	l.err = err
	l.closing = true
	l.stats.WriteErrors++
	l.stats.LostWrite += current
	for _, record := range l.queue {
		l.stats.LostWrite += record.events
	}
	l.queue = nil
	l.queueBytes = 0
	l.gap = nil
	callback := l.onError
	l.mu.Unlock()
	if callback != nil {
		callback(err)
	}
}

func (l *Logger) Stats() Stats {
	if l == nil {
		return Stats{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stats
}

func (l *Logger) Err() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *Logger) Close() error {
	if l == nil || (l.mode != Metadata && l.mode != Content) {
		return nil
	}
	l.mu.Lock()
	if !l.closing {
		l.closing = true
		l.wake()
	}
	done := l.done
	l.mu.Unlock()
	<-done
	return l.Err()
}
