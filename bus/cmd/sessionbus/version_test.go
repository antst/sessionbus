// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionDoesNotStartDaemonOrReadConfiguration(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("SESSIONBUS_HUB", "invalid:1")
	t.Setenv("SESSIONBUS_HUB_SECRET_FILE", "/not-present/private-secret")
	t.Setenv("SESSIONBUS_COMMS_LOG", "invalid-mode")
	oldVersion, oldRevision := buildVersion, buildRevision
	buildVersion, buildRevision = "v0.5.5", "0123456789abcdef"
	t.Cleanup(func() { buildVersion, buildRevision = oldVersion, oldRevision })
	for _, argument := range []string{"-version", "--version", "-v"} {
		var output bytes.Buffer
		if err := runTo([]string{argument}, &output); err != nil {
			t.Fatalf("%s: %v", argument, err)
		}
		if want := "sessionbus v0.5.5 (0123456789abcdef)\n"; output.String() != want {
			t.Fatalf("%s: got %q, want %q", argument, output.String(), want)
		}
	}
}

func TestDevelopmentVersionDoesNotInventRelease(t *testing.T) {
	if got := versionString(); !strings.HasPrefix(got, "sessionbus development (") || !strings.HasSuffix(got, ")") {
		t.Fatalf("unexpected development version: %q", got)
	}
}
