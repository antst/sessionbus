// SPDX-License-Identifier: MIT

package testsocket

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryUsesSelectedRuntimeParent(t *testing.T) {
	runtimeDirectory := t.TempDir()
	for _, test := range []struct {
		name, configured, want string
	}{{"configured runtime", runtimeDirectory, runtimeDirectory}, {"fallback", "", "/tmp"}} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("XDG_RUNTIME_DIR", test.configured)
			directory := Directory(t)
			if filepath.Dir(directory) != test.want {
				t.Fatalf("socket directory parent = %q, want %q", filepath.Dir(directory), test.want)
			}
			info, err := os.Stat(directory)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0o700 {
				t.Fatalf("socket directory mode = %v, want 0700", info.Mode().Perm())
			}
		})
	}
}
