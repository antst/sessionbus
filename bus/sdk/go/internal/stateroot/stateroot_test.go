// SPDX-License-Identifier: MIT

package stateroot

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/socketpath"
)

func TestSessionSocketUsesTheDocumentedDiscoveryOrder(t *testing.T) {
	t.Setenv("HOME", filepath.Join(t.TempDir(), "home"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "xdg"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "runtime"))
	explicit := filepath.Join(t.TempDir(), "session.sock")
	t.Setenv("SESSIONBUS_SOCKET", explicit)
	if got, err := SessionSocket(); err != nil || got != explicit {
		t.Fatalf("explicit socket = %q, %v", got, err)
	}

	t.Setenv("SESSIONBUS_SOCKET", "")
	xdg := filepath.Join(t.TempDir(), "selected-runtime")
	t.Setenv("XDG_RUNTIME_DIR", xdg)
	if got, err := SessionSocket(); err != nil || got != filepath.Join(xdg, "sessionbus", "presence.sock") {
		t.Fatalf("XDG socket = %q, %v", got, err)
	}
}

func TestDarwinDefaultRuntimePathFitsItsSocketLimit(t *testing.T) {
	root := filepath.Join("/tmp", "sessionbus-"+strings.Repeat("9", 10))
	lane := socketpath.Lane(filepath.Join(root, "presence.sock"), "qualified-session@host")
	if len([]byte(root)) > 59 || len([]byte(lane)) > 103 {
		t.Fatalf("Darwin runtime paths are too long: root=%q (%d), lane=%q (%d)", root, len(root), lane, len(lane))
	}
	t.Setenv("SESSIONBUS_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("TMPDIR", "/var/folders/zz/0123456789012345678901234567/T")
	got, err := RuntimeRoot()
	want := filepath.Join("/tmp", "sessionbus-"+strconv.Itoa(os.Getuid()))
	if err != nil || got != want {
		t.Fatalf("fallback runtime root = %q, %v", got, err)
	}
}
