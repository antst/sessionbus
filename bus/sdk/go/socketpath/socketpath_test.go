// SPDX-License-Identifier: MIT

package socketpath

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLaneUsesOneFixedLengthMappingForEverySessionID(t *testing.T) {
	socket := filepath.Join("/tmp", "sessionbus", "presence.sock")
	short := Lane(socket, "session")
	long := Lane(socket, strings.Repeat("qualified-session@remote-host", 20))
	if short == long || len(filepath.Base(short)) != 37 || len(filepath.Base(long)) != 37 {
		t.Fatalf("lane paths = %q, %q", short, long)
	}
	if short != Lane(socket, "session") {
		t.Fatal("lane mapping is not deterministic")
	}
}
