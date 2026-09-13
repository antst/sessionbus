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

func environmentWithout(names ...string) []string {
	blocked := make(map[string]struct{}, len(names))
	for _, name := range names {
		blocked[name] = struct{}{}
	}
	result := make([]string, 0, len(os.Environ()))
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if _, found := blocked[name]; !found {
			result = append(result, item)
		}
	}
	return result
}

func releaseFixture(t *testing.T, role string) (root, asset, marker string) {
	t.Helper()
	root = t.TempDir()
	payload := filepath.Join(root, "payload")
	if err := os.Mkdir(payload, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(payload, "ROLE"), []byte(role), 0600); err != nil {
		t.Fatal(err)
	}
	marker = filepath.Join(root, "installed")
	if err := os.WriteFile(filepath.Join(payload, "install"), []byte("#!/bin/sh\nprintf done > \"$INSTALL_TEST_MARKER\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	asset = "sessionbus-" + role + "-" + runtime.GOOS + "-" + runtime.GOARCH + ".tar.gz"
	archive := filepath.Join(root, asset)
	c := exec.Command("tar", "-czf", archive, "-C", payload, "ROLE", "install")
	if b, err := c.CombinedOutput(); err != nil {
		t.Fatalf("tar %v %s", err, b)
	}
	body, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	if err := os.WriteFile(filepath.Join(root, "SHA256SUMS"), []byte(digest+"  "+asset+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return root, asset, marker
}

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

func TestDownloadURLSelection(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		root     string
		wantBase string
	}{
		{
			name:     "stable default",
			wantBase: "https://github.com/antst/sessionbus/releases/latest/download",
		},
		{
			name:     "explicit latest",
			version:  "latest",
			wantBase: "https://github.com/antst/sessionbus/releases/latest/download",
		},
		{
			name:     "explicit development",
			version:  "development",
			wantBase: "https://github.com/antst/sessionbus/releases/download/development",
		},
		{
			name:     "explicit version",
			version:  "v0.4.0",
			wantBase: "https://github.com/antst/sessionbus/releases/download/v0.4.0",
		},
		{
			name:     "explicit mirror",
			version:  "v0.4.0",
			root:     "https://mirror.example/sessionbus/v0.4.0",
			wantBase: "https://mirror.example/sessionbus/v0.4.0",
		},
	}
	for _, role := range []string{"host", "hub"} {
		for _, test := range tests {
			t.Run(role+"/"+test.name, func(t *testing.T) {
				fixture, asset, marker := releaseFixture(t, role)
				fake := t.TempDir()
				logPath := filepath.Join(fake, "urls")
				curl := `#!/bin/sh
set -eu
url=
output=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) output=$2; shift 2 ;;
    --retry) shift 2 ;;
    -*) shift ;;
    *) url=$1; shift ;;
  esac
done
printf '%s\n' "$url" >> "$URL_LOG"
case "$url" in
  */SHA256SUMS) cp "$FIXTURE_ROOT/SHA256SUMS" "$output" ;;
  */"$FIXTURE_ASSET") cp "$FIXTURE_ROOT/$FIXTURE_ASSET" "$output" ;;
  *) exit 64 ;;
esac
`
				if err := os.WriteFile(filepath.Join(fake, "curl"), []byte(curl), 0700); err != nil {
					t.Fatal(err)
				}
				script := filepath.Join("..", "install-"+role+".sh")
				command := exec.Command("sh", script)
				command.Env = append(environmentWithout("SESSIONBUS_VERSION", "SESSIONBUS_DOWNLOAD_ROOT", "INSTALL_TEST_MARKER", "FIXTURE_ROOT", "FIXTURE_ASSET", "URL_LOG", "PATH"),
					"INSTALL_TEST_MARKER="+marker,
					"FIXTURE_ROOT="+fixture,
					"FIXTURE_ASSET="+asset,
					"URL_LOG="+logPath,
					"PATH="+fake+":"+os.Getenv("PATH"),
				)
				if test.version != "" {
					command.Env = append(command.Env, "SESSIONBUS_VERSION="+test.version)
				}
				if test.root != "" {
					command.Env = append(command.Env, "SESSIONBUS_DOWNLOAD_ROOT="+test.root)
				}
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("installer: %v\n%s", err, output)
				}
				urls, err := os.ReadFile(logPath)
				if err != nil {
					t.Fatal(err)
				}
				want := test.wantBase + "/" + asset + "\n" + test.wantBase + "/SHA256SUMS\n"
				if string(urls) != want {
					t.Fatalf("download URLs = %q, want %q", urls, want)
				}
				if body, err := os.ReadFile(marker); err != nil || string(body) != "done" {
					t.Fatalf("verified installer did not run: %q, %v", body, err)
				}
			})
		}
	}
}
