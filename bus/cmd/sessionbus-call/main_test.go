// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/daemon"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestOneShotResultAndError(t *testing.T) {
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "sessionbus.sock")
	service, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(directory, "sessions")})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-socket", socket, "-name", "probe", "session.list", `{}`}, &stdout, &stderr); code != 0 {
		t.Fatalf("list exit %d: %s / %s", code, stdout.String(), stderr.String())
	}
	var listed protocol.SessionListResult
	if err = json.Unmarshal(stdout.Bytes(), &listed); err != nil || len(listed.Sessions) != 1 || listed.Sessions[0].Name != "probe@local" {
		t.Fatalf("list = %#v, %v", listed, err)
	}

	stdout.Reset()
	if code := run([]string{"-socket", socket, "-g", "team", "turn.interrupt", `{"session_id":"missing"}`}, &stdout, &stderr); code != 1 {
		t.Fatalf("error exit = %d", code)
	}
	var rpcError protocol.RPCError
	if err = json.Unmarshal(stdout.Bytes(), &rpcError); err != nil || rpcError.Code != protocol.UnknownSession {
		t.Fatalf("error = %#v, %v (%s)", rpcError, err, stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	params := `{"target":"self","message":"hello"}`
	if code := run([]string{"-socket", socket, "-name", "self", "-g", "team", "message.send", params}, &stdout, &stderr); code != 0 {
		t.Fatalf("send exit %d: %s / %s", code, stdout.String(), stderr.String())
	}
	var delivery protocol.DeliveryRequest
	if err = json.Unmarshal(stderr.Bytes(), &delivery); err != nil || delivery.Body != "hello" {
		t.Fatalf("displayed delivery = %#v, %v", delivery, err)
	}
}

func TestUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"session.list", "{"}, &stdout, &stderr); code != 2 || stdout.Len() != 0 {
		t.Fatalf("usage = %d, %q, %q", code, stdout.String(), stderr.String())
	}
}

func TestSchemaErrorNamesPathAndConstraint(t *testing.T) {
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "sessionbus.sock")
	service, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(directory, "sessions")})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	var stdout, stderr bytes.Buffer
	params := `{"resume_session_id":"lane@local","name":"child"}`
	if code := run([]string{"-socket", socket, "lane.spawn", params}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d: %s / %s", code, stdout.String(), stderr.String())
	}
	var output map[string]string
	decodeErr := json.Unmarshal(stdout.Bytes(), &output)
	want := `LaneSpawnRequest: "name" is not allowed with "resume_session_id"`
	if decodeErr != nil || output["error"] != want || stderr.Len() != 0 {
		t.Fatalf("output = %q / %q", stdout.String(), stderr.String())
	}
}

func TestTurnRunWaitsForTerminal(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "sessionbus.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requests := make(chan protocol.Frame, 2)
	serverReady := make(chan net.Conn, 1)
	go func() {
		fd, _ := listener.Accept()
		serverReady <- fd
		reader := bufio.NewReader(fd)
		for {
			body, readErr := reader.ReadBytes('\n')
			if readErr != nil {
				return
			}
			frame, decodeErr := protocol.DecodeFrame(body[:len(body)-1])
			if decodeErr != nil {
				return
			}
			requests <- frame
		}
	}()
	var stdout, stderr bytes.Buffer
	finished := make(chan int, 1)
	go func() {
		params := `{"session_id":"lane@local","input":"work"}`
		finished <- run([]string{"-socket", socket, "turn.run", params}, &stdout, &stderr)
	}()
	server := <-serverReady
	hello := waitRequest(t, requests)
	if hello.Method != "session.hello" {
		t.Fatalf("first method = %s", hello.Method)
	}
	if err = writeResult(server, hello, struct{}{}); err != nil {
		t.Fatal(err)
	}
	runRequest := waitRequest(t, requests)
	if runRequest.Method != "turn.run" {
		t.Fatalf("second method = %s", runRequest.Method)
	}
	select {
	case code := <-finished:
		t.Fatalf("returned before terminal with exit %d", code)
	default:
	}
	terminal := protocol.RunStatus{SessionID: "lane@local", RunID: "g/1", State: "done", Result: &protocol.TurnResult{Outcome: "completed", Result: "terminal"}}
	if err = writeResult(server, runRequest, terminal); err != nil {
		t.Fatal(err)
	}
	if code := <-finished; code != 0 || stdout.String() != `{"session_id":"lane@local","run_id":"g/1","state":"done","result":{"outcome":"completed","result":"terminal"}}`+"\n" {
		t.Fatalf("terminal exit %d: %s / %s", code, stdout.String(), stderr.String())
	}
}

func waitRequest(t *testing.T, requests <-chan protocol.Frame) protocol.Frame {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("request timed out")
		return protocol.Frame{}
	}
}

func writeResult(fd net.Conn, request protocol.Frame, result any) error {
	body, err := protocol.ResultBytes(request.ID, request.Method, result)
	if err == nil {
		_, err = fd.Write(body)
	}
	return err
}

func TestTimeoutIncludesMissingDaemon(t *testing.T) {
	var stdout, stderr bytes.Buffer
	socket := filepath.Join(testsocket.Directory(t), "absent.sock")
	done := make(chan int, 1)
	go func() { done <- run([]string{"-socket", socket, "-timeout", "5ms", "session.list"}, &stdout, &stderr) }()
	select {
	case code := <-done:
		if code != 1 || !bytes.Contains(stdout.Bytes(), []byte("context deadline exceeded")) {
			t.Fatalf("%d %s %s", code, stdout.String(), stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection deadline did not stop CLI")
	}
}

func TestTimeoutClosesBlockedWrite(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "blocked.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var stdout, stderr bytes.Buffer
	finished := make(chan int, 1)
	go func() {
		params, _ := json.Marshal(map[string]string{"target": "someone", "message": strings.Repeat("😀", 200000)})
		finished <- run([]string{"-socket", socket, "-timeout", "1s", "message.send", string(params)}, &stdout, &stderr)
	}()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.(*net.UnixConn).SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(server)
	body, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	hello, err := protocol.DecodeFrame(body[:len(body)-1])
	if err != nil {
		t.Fatal(err)
	}
	if err := writeResult(server, hello, struct{}{}); err != nil {
		t.Fatal(err)
	}
	// Observe the next write starting, then stop draining its large body.
	if _, err := reader.ReadByte(); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-finished:
		if code != 1 {
			t.Fatalf("unexpected exit %d: %s", code, stdout.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadline failed to unblock transport write")
	}
}
