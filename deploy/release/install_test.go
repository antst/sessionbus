// SPDX-License-Identifier: GPL-3.0-only
package release_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0700); err != nil {
		t.Fatal(err)
	}
}

func TestRealRecipeReinstallPreservesKeysAndOtherRole(t *testing.T) {
	script, err := os.ReadFile("install")
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"host", "hub"} {
		t.Run(role, func(t *testing.T) {
			home := t.TempDir()
			src := filepath.Join(t.TempDir(), "source with spaces")
			fake := t.TempDir()
			write(t, filepath.Join(src, "install"), string(script))
			write(t, filepath.Join(src, "ROLE"), role)
			write(t, filepath.Join(src, "REVISION"), "abcdef123456")
			write(t, filepath.Join(src, "LICENSE"), "license")
			write(t, filepath.Join(src, "THIRD-PARTY-NOTICES.txt"), "notices")
			cmds := []string{"sessionbus-hub"}
			if role == "host" {
				cmds = []string{"sessionbus", "sessionbus-call", "example-peer"}
			}
			for _, name := range cmds {
				write(t, filepath.Join(src, "bin", name), "#!/bin/sh\n[ \"${1:-}\" != secret ] || printf 'fixed-secret\\n'\nexit 0\n")
			}
			write(t, filepath.Join(fake, "uname"), "#!/bin/sh\nprintf 'Linux\\n'\n")
			write(t, filepath.Join(fake, "systemctl"), "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/service-calls\"\nif [ \"$2\" = cat ]; then test -f \"$HOME/.config/systemd/user/$3\"; fi\n")
			// An unrelated role is installed already; the recipe must leave it alone.
			other := "hub"
			if role == "hub" {
				other = "host"
			}
			sentinel := filepath.Join(home, ".local/libexec/sessionbus", other, "keep")
			write(t, sentinel, "unchanged")
			command := func() {
				t.Helper()
				c := exec.Command("sh", filepath.Join(src, "install"))
				c.Env = append(os.Environ(), "HOME="+home, "XDG_CONFIG_HOME="+filepath.Join(home, ".config"), "PATH="+fake+":"+os.Getenv("PATH"))
				if out, err := c.CombinedOutput(); err != nil {
					t.Fatalf("install: %v\n%s", err, out)
				}
			}
			// Earlier manual installers used a real current directory.
			old := filepath.Join(home, ".local/libexec/sessionbus", role, "current", "keep-prior")
			write(t, old, "prior-payload")
			command()
			preserved, err := filepath.Glob(filepath.Join(home, ".local/libexec/sessionbus", role, "releases/prior.*/current/keep-prior"))
			if err != nil || len(preserved) != 1 {
				t.Fatalf("prior directory lost: %v %v", preserved, err)
			}
			b, err := os.ReadFile(preserved[0])
			if err != nil || string(b) != "prior-payload" {
				t.Fatal("prior payload changed", err)
			}
			key := filepath.Join(home, ".config/sessionbus/host.key")
			if role == "hub" {
				key = filepath.Join(home, ".config/sessionbus/hub.json")
			}
			if err := os.WriteFile(key, []byte("preserve-existing"), 0600); err != nil {
				t.Fatal(err)
			}
			command()
			b, err = os.ReadFile(key)
			if err != nil || string(b) != "preserve-existing" {
				t.Fatal("secret overwritten", err)
			}
			b, err = os.ReadFile(sentinel)
			if err != nil || string(b) != "unchanged" {
				t.Fatal("other role changed", err)
			}
			target, err := filepath.EvalSymlinks(filepath.Join(home, ".local/libexec/sessionbus", role, "current"))
			if err != nil || !strings.HasSuffix(target, "/releases/abcdef123456") {
				t.Fatal("current link", target, err)
			}
			st, err := os.Stat(key)
			if err != nil || st.Mode().Perm() != 0600 {
				t.Fatal("secret permissions", err)
			}
		})
	}
}
