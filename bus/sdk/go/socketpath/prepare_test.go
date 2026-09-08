// SPDX-License-Identifier: MIT

package socketpath

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPrepareRejectsTheFirstUnusableByte(t *testing.T) {
	limit := socketLimit()
	parent, err := os.MkdirTemp("/tmp", "sb-edge-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	rootLength := limit + 1 - len("/lanes/") - 32 - len(".sock")
	root := filepath.Join(parent, strings.Repeat("r", rootLength-len(parent)-1))
	socket := filepath.Join(root, "presence.sock")
	lane := Lane(socket, "")
	if len([]byte(lane)) != limit+1 {
		t.Fatalf("boundary setup lane length = %d, want %d", len(lane), limit+1)
	}
	err = Prepare(socket)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("limit %d exceeded by %q", limit, lane)) {
		t.Fatalf("boundary error = %v", err)
	}
	if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
		t.Fatalf("runtime root created before rejection: %v", statErr)
	}
}

func TestPrepareRejectsAnUnusablePresencePathBeforeCreation(t *testing.T) {
	limit := socketLimit()
	base, err := os.MkdirTemp("/tmp", "sb-presence-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	root := filepath.Join(base, "runtime")
	socket := filepath.Join(root, strings.Repeat("s", limit+1-len(root)-1))
	if len([]byte(socket)) != limit+1 {
		t.Fatalf("boundary setup presence length = %d, want %d", len(socket), limit+1)
	}
	err = Prepare(socket)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("limit %d exceeded by %q", limit, socket)) {
		t.Fatalf("presence boundary error = %v", err)
	}
	if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
		t.Fatalf("runtime root created before rejection: %v", statErr)
	}
}

func TestPrepareDoesNotFollowFinalSymlink(t *testing.T) {
	base, err := os.MkdirTemp("/tmp", "sb-link-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	victim, root := filepath.Join(base, "victim"), filepath.Join(base, "runtime")
	if err = os.Mkdir(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(victim, root); err != nil {
		t.Fatal(err)
	}
	if err = Prepare(filepath.Join(root, "presence.sock")); err == nil {
		t.Fatal("accepted final-component symlink")
	}
	info, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("symlink target mode = %v", info.Mode().Perm())
	}
}

func TestPrepareSecuresRuntimeDirectory(t *testing.T) {
	parent, err := os.MkdirTemp("/tmp", "sb-root-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	root := filepath.Join(parent, "runtime")
	if err = os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err = Prepare(filepath.Join(root, "presence.sock")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("runtime directory mode = %v", info.Mode().Perm())
	}
}

func socketLimit() int {
	if runtime.GOOS == "darwin" {
		return 103
	}
	return 107
}
