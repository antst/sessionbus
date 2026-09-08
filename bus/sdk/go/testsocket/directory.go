// SPDX-License-Identifier: MIT

// Package testsocket provides short-lived directories for Unix-socket tests.
package testsocket

import (
	"os"
	"runtime"
)

type testCleanup interface {
	Helper()
	Cleanup(func())
	Fatalf(string, ...any)
}

// Directory returns a unique, short runtime directory and removes it after the test.
func Directory(t testCleanup) string {
	t.Helper()
	parent := "/tmp"
	if runtime.GOOS != "darwin" {
		if runtimeDirectory := os.Getenv("XDG_RUNTIME_DIR"); runtimeDirectory != "" {
			parent = runtimeDirectory
		}
	}
	directory, err := os.MkdirTemp(parent, "sb-test-")
	if err != nil {
		t.Fatalf("create short socket directory: %v", err)
		return ""
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}
