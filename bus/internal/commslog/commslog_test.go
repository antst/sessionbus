// SPDX-License-Identifier: GPL-3.0-only

package commslog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetadataFiltersAndOmitsContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	logger := openTestLogger(t, Options{
		Mode: Metadata, Path: path, Host: "alpha", Incarnation: "daemon-1",
		MaxFileBytes: 1 << 20, MaxFiles: 2, QueueBytes: 1 << 20,
		Sessions: []string{"session-1"}, Groups: []string{"group-1"},
	})
	if logger.Emit(Event{Type: Request, Method: MessageSend, Body: "filtered secret",
		From: &Endpoint{SessionID: "other", Groups: []string{"other"}}}) {
		t.Fatal("unmatched event was admitted")
	}
	if !logger.Emit(Event{Type: Request, Method: MessageSend, MessageID: "message-1",
		Body: "private body", State: Running,
		From: &Endpoint{SessionID: "session-1", Product: "codex", Name: "one", Groups: []string{"group-2"}},
		To:   &Endpoint{Host: "beta", Target: "worker", Targets: []string{"worker", "reviewer"}}}) {
		t.Fatal("matching event was not admitted")
	}
	if !logger.Emit(Event{Type: Lifecycle, Method: ConnectionClosed, State: Disconnected,
		Body: "must be stripped before validation",
		To:   &Endpoint{SessionID: "other", Groups: []string{"group-1"}}}) {
		t.Fatal("group-matching event was not admitted")
	}
	if logger.Emit(Event{Type: Request, From: &Endpoint{SessionID: "session-1"}}) {
		t.Fatal("empty method was admitted")
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	raw := mustRead(t, path)
	if bytes.Contains(raw, []byte("private body")) || bytes.Contains(raw, []byte("filtered secret")) {
		t.Fatalf("metadata log contains message body: %s", raw)
	}
	records := decodeRecords(t, raw)
	if len(records) != 2 || records[0].Sequence != 1 || records[1].Sequence != 2 {
		t.Fatalf("records = %+v", records)
	}
	if records[0].Host != "alpha" || records[0].Incarnation != "daemon-1" || records[0].From.Product != "codex" || records[0].From.Name != "one" {
		t.Fatalf("metadata record = %+v", records[0])
	}
	if got := records[0].To.Targets; len(got) != 2 || got[0] != "worker" || got[1] != "reviewer" {
		t.Fatalf("targets = %#v", got)
	}
	stats := logger.Stats()
	if stats.LastSequence != 2 || stats.Written != 2 || stats.LostInvalid != 1 || stats.LostOverflow != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestContentModeIncludesOnlyTypedBody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	logger := openTestLogger(t, Options{Mode: Content, Path: path, Host: "local", Incarnation: "one",
		MaxFileBytes: 1 << 20, MaxFiles: 1, QueueBytes: 1 << 20})
	if !logger.Emit(Event{Type: Request, Method: MessageSend, MessageID: "message-1",
		DeliveryID: "delivery-1", Body: "hello\nworld", From: &Endpoint{SessionID: "from@local"},
		To: &Endpoint{SessionID: "to@local"}}) {
		t.Fatal("content event was not admitted")
	}
	if logger.Emit(Event{Type: Request, Method: TurnExecute, Body: "native prompt"}) {
		t.Fatal("content mode admitted a body outside message.send")
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	records := decodeRecords(t, mustRead(t, path))
	if len(records) != 1 || records[0].Body != "hello\nworld" || records[0].DeliveryID != "delivery-1" {
		t.Fatalf("record = %+v", records)
	}
	if records[0].UTC.Location() != time.UTC {
		t.Fatalf("timestamp is not UTC: %v", records[0].UTC)
	}
	if logger.Stats().LostInvalid != 1 {
		t.Fatalf("invalid counter = %+v", logger.Stats())
	}
}

func TestReceiptValidationMatchesWireGrammar(t *testing.T) {
	sink := &memorySink{}
	logger := newLogger(Options{Mode: Metadata, Host: "local", Incarnation: "one",
		MaxFileBytes: 1 << 20, MaxFiles: 1, QueueBytes: 1 << 20}, sink)
	for _, disposition := range []string{ReceiptWritten, ReceiptInjected, ReceiptQueuedForNextTurn} {
		if !logger.Emit(Event{Type: Receipt, Method: MessageReceipt,
			Receipt: &DeliveryReceipt{Disposition: disposition}}) {
			t.Fatalf("valid %q receipt was rejected", disposition)
		}
	}
	if !logger.Emit(Event{Type: Receipt, Method: MessageReceipt,
		Receipt: &DeliveryReceipt{Disposition: ReceiptRejected, Reason: "no_receipt"}}) {
		t.Fatal("valid rejected receipt was rejected")
	}
	for _, receipt := range []*DeliveryReceipt{
		{Disposition: ReceiptWritten, Reason: "unexpected"},
		{Disposition: ReceiptRejected},
		{Disposition: "unknown"},
	} {
		if logger.Emit(Event{Type: Receipt, Method: MessageReceipt, Receipt: receipt}) {
			t.Fatalf("invalid receipt was admitted: %+v", receipt)
		}
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	stats := logger.Stats()
	if stats.Written != 4 || stats.LostInvalid != 3 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestConcurrentEmissionHasOrderedSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	logger := openTestLogger(t, Options{Mode: Metadata, Path: path, Host: "local", Incarnation: "one",
		MaxFileBytes: 8 << 20, MaxFiles: 1, QueueBytes: 8 << 20})
	const goroutines, each = 12, 100
	var group sync.WaitGroup
	group.Add(goroutines)
	for index := 0; index < goroutines; index++ {
		go func(index int) {
			defer group.Done()
			for item := 0; item < each; item++ {
				if !logger.Emit(Event{Type: Request, Method: TurnExecute,
					RunID: fmt.Sprintf("run-%d-%d", index, item)}) {
					t.Errorf("event %d/%d was not admitted", index, item)
					return
				}
			}
		}(index)
	}
	group.Wait()
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	records := decodeRecords(t, mustRead(t, path))
	if len(records) != goroutines*each {
		t.Fatalf("record count = %d", len(records))
	}
	for index, record := range records {
		if record.Sequence != uint64(index+1) {
			t.Fatalf("record %d sequence = %d", index, record.Sequence)
		}
	}
}

func TestOverflowWritesGapAndCountsLoss(t *testing.T) {
	sink := newBlockingSink()
	logger := newLogger(Options{Mode: Content, Host: "local", Incarnation: "one",
		MaxFileBytes: 1 << 20, MaxFiles: 1, QueueBytes: minimumQueueBytes}, sink)
	body := strings.Repeat("x", 1200)
	if !logger.Emit(Event{Type: Request, Method: MessageSend, Body: body}) {
		t.Fatal("first event was not admitted")
	}
	<-sink.entered
	for index := 0; index < 20; index++ {
		logger.Emit(Event{Type: Request, Method: MessageSend, Body: body, MessageID: fmt.Sprint(index)})
	}
	close(sink.release)
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	stats := logger.Stats()
	if stats.LostOverflow == 0 || stats.GapRecords != 1 || stats.WriteErrors != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	records := decodeRecords(t, sink.Bytes())
	var gaps int
	for _, record := range records {
		if record.Gap != nil {
			gaps++
			if record.Gap.Cause != "queue_overflow" || record.Gap.Count != stats.LostOverflow || record.Gap.FirstSequence > record.Gap.LastSequence {
				t.Fatalf("gap = %+v, stats = %+v", record.Gap, stats)
			}
		}
	}
	if gaps != 1 {
		t.Fatalf("gap records = %d in %+v", gaps, records)
	}
}

func TestWriteFailureIsVisibleAndJoins(t *testing.T) {
	want := errors.New("injected write failure")
	sink := &failingSink{writeErr: want, entered: make(chan struct{}), release: make(chan struct{})}
	var callbackMu sync.Mutex
	var callbacks []error
	logger := newLogger(Options{Mode: Metadata, Host: "local", Incarnation: "one",
		MaxFileBytes: 1 << 20, MaxFiles: 1, QueueBytes: 1 << 20,
		OnError: func(err error) {
			callbackMu.Lock()
			callbacks = append(callbacks, err)
			callbackMu.Unlock()
		}}, sink)
	if !logger.Emit(Event{Type: Lifecycle, Method: DaemonStart, RunID: "0"}) {
		t.Fatal("event 0 was not admitted")
	}
	<-sink.entered
	for index := 1; index < 3; index++ {
		if !logger.Emit(Event{Type: Lifecycle, Method: DaemonStart, RunID: fmt.Sprint(index)}) {
			t.Fatalf("event %d was not admitted", index)
		}
	}
	close(sink.release)
	if err := logger.Close(); !errors.Is(err, want) {
		t.Fatalf("Close error = %v", err)
	}
	if !errors.Is(logger.Err(), want) {
		t.Fatalf("Err = %v", logger.Err())
	}
	stats := logger.Stats()
	if stats.WriteErrors != 1 || stats.LostWrite != 3 || stats.Written != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	callbackMu.Lock()
	defer callbackMu.Unlock()
	if len(callbacks) != 1 || !errors.Is(callbacks[0], want) {
		t.Fatalf("callbacks = %v", callbacks)
	}
	if sink.closeCount != 1 {
		t.Fatalf("sink close count = %d", sink.closeCount)
	}
}

func TestCloseFlushesAcceptedEventsAndIsIdempotent(t *testing.T) {
	sink := &memorySink{}
	logger := newLogger(Options{Mode: Metadata, Host: "local", Incarnation: "one",
		MaxFileBytes: 1 << 20, MaxFiles: 1, QueueBytes: 1 << 20}, sink)
	for index := 0; index < 20; index++ {
		if !logger.Emit(Event{Type: Receipt, Method: MessageReceipt, MessageID: fmt.Sprint(index),
			Outcome: Completed, Receipt: &DeliveryReceipt{DeliveryID: fmt.Sprint(index), Disposition: "written"}}) {
			t.Fatalf("event %d was not admitted", index)
		}
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	if logger.Emit(Event{Type: Lifecycle, Method: DaemonStop}) {
		t.Fatal("event was admitted after Close")
	}
	if got := len(decodeRecords(t, sink.Bytes())); got != 20 {
		t.Fatalf("record count = %d", got)
	}
	if sink.closeCount != 1 {
		t.Fatalf("sink close count = %d", sink.closeCount)
	}
}

func TestOffModeNeedsNoPath(t *testing.T) {
	logger, err := Open(Options{Mode: Off})
	if err != nil {
		t.Fatal(err)
	}
	if logger.Emit(Event{Type: Lifecycle, Method: DaemonStart}) {
		t.Fatal("off logger admitted event")
	}
	if err = logger.Close(); err != nil {
		t.Fatal(err)
	}
}

type memorySink struct {
	mu         sync.Mutex
	buffer     bytes.Buffer
	closeCount int
}

func (s *memorySink) WriteRecord(raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.buffer.Write(raw)
	return err
}

func (s *memorySink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCount++
	return nil
}

func (s *memorySink) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.buffer.Bytes()...)
}

type blockingSink struct {
	memorySink
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingSink() *blockingSink {
	return &blockingSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingSink) WriteRecord(raw []byte) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.memorySink.WriteRecord(raw)
}

type failingSink struct {
	writeErr   error
	closeErr   error
	closeCount int
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (s *failingSink) WriteRecord([]byte) error {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.writeErr
}
func (s *failingSink) Close() error {
	s.closeCount++
	return s.closeErr
}

func openTestLogger(t *testing.T, options Options) *Logger {
	t.Helper()
	if err := os.Chmod(filepath.Dir(options.Path), 0o700); err != nil {
		t.Fatal(err)
	}
	logger, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close() })
	return logger
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decodeRecords(t *testing.T, raw []byte) []diskRecord {
	t.Helper()
	var records []diskRecord
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	buffer := make([]byte, 1024)
	scanner.Buffer(buffer, 2<<20)
	for scanner.Scan() {
		var record diskRecord
		if err := jsonUnmarshalStrict(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode record %q: %v", scanner.Bytes(), err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

func jsonUnmarshalStrict(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}
