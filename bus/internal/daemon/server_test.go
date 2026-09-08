// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/socketpath"
)

func TestStartSweepsStaleSocketsAndLeavesLiveOnes(t *testing.T) {
	directory := shortTempDir(t)
	socket := filepath.Join(directory, "sessionbus.sock")
	lanes := filepath.Join(directory, "lanes")
	if err := os.Mkdir(lanes, 0o700); err != nil {
		t.Fatal(err)
	}
	leaveStaleSocket(t, socket)
	staleLane := filepath.Join(lanes, "stale.sock")
	leaveStaleSocket(t, staleLane)
	liveLane, err := net.Listen("unix", filepath.Join(lanes, "live.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer liveLane.Close()
	daemon, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(directory, "sessions")})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	if _, err = os.Stat(staleLane); !os.IsNotExist(err) {
		t.Fatalf("stale lane remains: %v", err)
	}
	if fd, dialErr := net.Dial("unix", liveLane.Addr().String()); dialErr != nil {
		t.Fatalf("live lane removed: %v", dialErr)
	} else {
		_ = fd.Close()
	}
}

func TestStartRejectsPartialFederationConfiguration(t *testing.T) {
	for _, values := range []struct{ host, address, secret string }{
		{host: "alpha", address: "hub:9443"},
		{host: "alpha", secret: "secret"},
		{host: "local", address: "hub:9443", secret: "secret"},
	} {
		directory := t.TempDir()
		_, err := Start(Config{SocketPath: filepath.Join(directory, "sessionbus.sock"), TablePath: filepath.Join(directory, "sessions"), Host: values.host, HubAddress: values.address, HubSecret: values.secret})
		if err == nil {
			t.Fatalf("accepted partial federation config %#v", values)
		}
	}
}

func TestStartRejectsRuntimeRootThatCannotFitLaneSocket(t *testing.T) {
	limit := 107
	if runtime.GOOS == "darwin" {
		limit = 103
	}
	parent := shortTempDir(t)
	root := filepath.Join(parent, strings.Repeat("x", limit-20-len(parent)-1))
	socket := filepath.Join(root, "presence.sock")
	table := filepath.Join(t.TempDir(), "sessions")
	lane := socketpath.Lane(socket, "")
	_, err := Start(Config{SocketPath: socket, TablePath: table})
	if err == nil || err.Error() != "sessionbus Unix socket path limit "+fmt.Sprint(limit)+" exceeded by \""+lane+"\"" {
		t.Fatalf("start error = %v", err)
	}
	if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
		t.Fatalf("runtime root was created before rejection: %v", statErr)
	}
}

func leaveStaleSocket(t *testing.T, path string) {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
}
