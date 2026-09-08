// SPDX-License-Identifier: MIT

package testsocket

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDirectoryUsesPlatformRuntimeParent(t *testing.T) {
	runtimeDirectory := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeDirectory)
	directory := Directory(t)
	wantParent := runtimeDirectory
	if runtime.GOOS == "darwin" {
		wantParent = "/tmp"
	}
	if filepath.Dir(directory) != wantParent {
		t.Fatalf("socket directory parent = %q, want %q", filepath.Dir(directory), wantParent)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("socket directory mode = %v, want 0700", info.Mode().Perm())
	}
}
