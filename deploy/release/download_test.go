// SPDX-License-Identifier: GPL-3.0-only
package release_test

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDownloadRequiresExactChecksumBeforeInstallation(t *testing.T) {
	for _, role := range []string{"host", "hub"} {
		t.Run(role, func(t *testing.T) {
			root := t.TempDir()
			payload := filepath.Join(root, "payload")
			if err := os.Mkdir(payload, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(payload, "ROLE"), []byte(role), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(payload, "install"), []byte("#!/bin/sh\nprintf done > \"$INSTALL_TEST_MARKER\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			asset := "sessionbus-" + role + "-" + runtime.GOOS + "-" + runtime.GOARCH + ".tar.gz"
			archive := filepath.Join(root, asset)
			c := exec.Command("tar", "-czf", archive, "-C", payload, "ROLE", "install")
			if b, err := c.CombinedOutput(); err != nil {
				t.Fatalf("tar %v %s", err, b)
			}
			b, err := os.ReadFile(archive)
			if err != nil {
				t.Fatal(err)
			}
			digest := fmt.Sprintf("%x", sha256.Sum256(b))
			marker := filepath.Join(root, "installed")
			script := filepath.Join("..", "install-"+role+".sh")
			run := func() ([]byte, error) {
				c := exec.Command("sh", script)
				c.Env = append(os.Environ(), "SESSIONBUS_DOWNLOAD_ROOT="+(&url.URL{Scheme: "file", Path: root}).String(), "INSTALL_TEST_MARKER="+marker)
				return c.CombinedOutput()
			}
			for _, sum := range []string{strings.Repeat("0", 64), digest + "  " + asset + "\n" + digest} {
				if err := os.WriteFile(filepath.Join(root, "SHA256SUMS"), []byte(sum+"  "+asset+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if b, err := run(); err == nil {
					t.Fatalf("bad checksum accepted: %s", b)
				}
				if _, err := os.Stat(marker); !os.IsNotExist(err) {
					t.Fatal("installer ran before verification")
				}
			}
			if err := os.WriteFile(filepath.Join(root, "SHA256SUMS"), []byte(digest+"  "+asset+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if b, err := run(); err != nil {
				t.Fatalf("verified download: %v %s", err, b)
			}
			if b, err := os.ReadFile(marker); err != nil || string(b) != "done" {
				t.Fatal("installer missing", err)
			}
		})
	}
}
