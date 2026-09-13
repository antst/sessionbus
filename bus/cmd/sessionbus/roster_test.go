// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"slices"
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

func TestRosterCommandActiveDefaultAndAll(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		for _, all := range []bool{false, true} {
			for _, localOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("json=%t/all=%t/local=%t", asJSON, all, localOnly), func(t *testing.T) {
					socket := filepath.Join(testsocket.Directory(t), "presence.sock")
					listener, err := net.Listen("unix", roster.Socket(socket))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = listener.Close() })
					if err := listener.(*net.UnixListener).SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
						t.Fatal(err)
					}
					rows := []roster.Row{
						{SessionID: "online-idle", Kind: "peer", Connected: true},
						{SessionID: "online-lane", Kind: "lane", Connected: true, Persistent: true},
						{SessionID: "active-run", Kind: "lane", Running: true},
						{SessionID: "offline-peer", Kind: "peer"},
						{SessionID: "archived-lane", Kind: "lane", Persistent: true},
					}
					done := make(chan error, 1)
					go func() {
						fd, err := listener.Accept()
						if err != nil {
							done <- err
							return
						}
						defer fd.Close()
						_ = fd.SetDeadline(time.Now().Add(3 * time.Second))
						var request roster.Request
						if err = json.NewDecoder(fd).Decode(&request); err != nil {
							done <- err
							return
						}
						if request.Local != localOnly {
							done <- fmt.Errorf("local request = %t", request.Local)
							return
						}
						value := roster.Report{Schema: roster.Schema, Complete: localOnly, Local: roster.Host{Host: "local", Sessions: rows}, Remote: []roster.Host{}}
						if !localOnly {
							value.Remote = []roster.Host{
								{Host: "remote", Sessions: rows},
								{Host: "offline", Sessions: rows[3:]},
								{Host: "legacy", Error: "upgrade_host_for_roster", Sessions: []roster.Row{}},
							}
						}
						done <- json.NewEncoder(fd).Encode(value)
					}()
					args := []string{"--socket", socket, "--timeout", "3s"}
					for flag, enabled := range map[string]bool{"--json": asJSON, "--all": all, "--local": localOnly} {
						if enabled {
							args = append(args, flag)
						}
					}
					var out bytes.Buffer
					err = runRoster(args, &out)
					if serverErr := <-done; serverErr != nil {
						t.Fatal(serverErr)
					}
					if localOnly && err != nil || !localOnly && (err == nil || !strings.Contains(err.Error(), "incomplete")) {
						t.Fatalf("roster error = %v", err)
					}
					if asJSON {
						var got roster.Report
						if err := json.Unmarshal(out.Bytes(), &got); err != nil {
							t.Fatal(err)
						}
						wanted := rows[:3]
						if all {
							wanted = rows
						}
						if !slices.EqualFunc(got.Local.Sessions, wanted, func(a, b roster.Row) bool { return a.SessionID == b.SessionID }) {
							t.Fatalf("local rows = %#v", got.Local.Sessions)
						}
						if !localOnly {
							if !slices.EqualFunc(got.Remote[0].Sessions, wanted, func(a, b roster.Row) bool { return a.SessionID == b.SessionID }) || got.Complete || got.Remote[2].Error != "upgrade_host_for_roster" {
								t.Fatalf("remote report = %#v", got)
							}
							if !all && (got.Remote[1].Sessions == nil || len(got.Remote[1].Sessions) != 0) {
								t.Fatal("filtered host must retain an empty JSON array")
							}
						}
					} else {
						for i, row := range rows {
							if strings.Contains(out.String(), row.SessionID) != (all || i < 3) {
								t.Fatalf("row visibility differs: %s: %s", row.SessionID, out.String())
							}
						}
						if !localOnly && !strings.Contains(out.String(), "upgrade_host_for_roster") {
							t.Fatal("filter hid remote error")
						}
					}
				})
			}
		}
	}
}

func TestRosterHumanEscapesTerminalControls(t *testing.T) {
	var out bytes.Buffer
	value := roster.Report{Local: roster.Host{Host: "local", Sessions: []roster.Row{{Name: "bad\x1b[2J\nrow\tcolumn\u009b31m\u0085next", Groups: []string{"one\rspoof"}}}}}
	if err := renderRoster(&out, value); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(out.String(), "\x1b\r\u009b\u0085") {
		t.Fatalf("terminal controls escaped rendering: %q", out.String())
	}
}
