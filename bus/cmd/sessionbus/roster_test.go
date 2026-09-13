// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/daemon"
	"github.com/antst/sessionbus/bus/internal/roster"
	sdk "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestRosterCommandHumanJSONAndNoRegisteredObserver(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "presence.sock")
	d, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(t.TempDir(), "rows")})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	t.Setenv("SESSIONBUS_SOCKET", socket)
	peer, err := sdk.ConnectPeer(sdk.PeerIdentity{SessionID: "hidden", Product: "fixture", Name: "My session", Groups: []string{"private-team"}, Info: map[string]any{}}, func(context.Context, sdk.PeerIdentity, sdk.DeliveryRequest) (sdk.DeliveryReceipt, error) {
		return sdk.DeliveryReceipt{Disposition: "written"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Shutdown()
	select {
	case <-peer.Ready():
	case <-time.After(time.Second):
		t.Fatal("peer not ready")
	}
	t.Setenv("SESSIONBUS_SOCKET", socket) // SDK connection consumes its launch environment.
	var jsonOut bytes.Buffer
	if err = runTo([]string{"roster", "--json"}, &jsonOut); err != nil {
		t.Fatal(err)
	}
	var result roster.Report
	if err = json.Unmarshal(jsonOut.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Complete || len(result.Local.Sessions) != 1 || result.Local.Sessions[0].SessionID != "hidden@local" {
		t.Fatalf("roster=%#v", result)
	}
	var human bytes.Buffer
	if err = runTo([]string{"roster", "--socket", socket}, &human); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"HOST", "SESSION ID", "My session@local", "hidden@local", "private-team", "idle"} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("missing %s: %s", want, human.String())
		}
	}
	for _, args := range [][]string{{"--timeout", "0"}, {"unexpected"}} {
		if runRoster(args, &bytes.Buffer{}) == nil {
			t.Fatal("accepted invalid args", args)
		}
	}
	if err = runRoster([]string{"--help"}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
}

func TestRosterHumanEscapesTerminalControls(t *testing.T) {
	var out bytes.Buffer
	value := roster.Report{Local: roster.Host{Host: "local", Sessions: []roster.Row{{Name: "bad\x1b[2J\nrow\tcolumn", Groups: []string{"one\rspoof"}}}}}
	if err := renderRoster(&out, value); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(out.String(), "\x1b\r") {
		t.Fatalf("terminal controls escaped rendering: %q", out.String())
	}
}
