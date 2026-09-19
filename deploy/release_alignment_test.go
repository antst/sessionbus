// SPDX-License-Identifier: GPL-3.0-only

package deploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func releaseCommand(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func releaseFixture(t *testing.T) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("release helpers require Python 3")
	}
	root := t.TempDir()
	repo, remote := filepath.Join(root, "repo"), filepath.Join(root, "origin.git")
	for _, dir := range []string{filepath.Join(repo, "deploy"), filepath.Join(repo, "bus")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, script := range []string{"check-release-tag", "publish-go-tag"} {
		body, err := os.ReadFile(script)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, "deploy", script), body, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "bus/package.json"), []byte("{\"version\":\"0.5.5\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	releaseCommand(t, root, "git", "init", "--bare", remote)
	releaseCommand(t, repo, "git", "init")
	for key, value := range map[string]string{"user.name": "Release test", "user.email": "release-test@example.invalid", "commit.gpgsign": "false", "tag.gpgsign": "false"} {
		releaseCommand(t, repo, "git", "config", key, value)
	}
	releaseCommand(t, repo, "git", "add", ".")
	releaseCommand(t, repo, "git", "commit", "-m", "fixture")
	releaseCommand(t, repo, "git", "remote", "add", "origin", remote)
	return repo, remote
}

func TestReleaseVersionRejectsMismatchesAndPrereleases(t *testing.T) {
	repo, _ := releaseFixture(t)
	releaseCommand(t, repo, "deploy/check-release-tag", "v0.5.5")
	for _, tag := range []string{"v0.5.4", "kit-v0.5.5", "v0.5.5-pre.1", "v0.05.5", "v0.5.5/extra"} {
		cmd := exec.Command("deploy/check-release-tag", tag)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("accepted %q: %s", tag, out)
		}
	}
}

func TestGoModuleTagUsesReleaseCommitAndNeverMovesIt(t *testing.T) {
	for _, existing := range []string{"none", "lightweight", "annotated"} {
		t.Run(existing, func(t *testing.T) {
			repo, remote := releaseFixture(t)
			const tag = "bus/sdk/go/v0.5.5"
			want := releaseCommand(t, repo, "git", "rev-parse", "HEAD")
			if existing != "none" {
				args := []string{"git", "tag", tag}
				if existing == "annotated" {
					args = append(args, "-a", "-m", "existing tag")
				}
				releaseCommand(t, repo, args...)
				releaseCommand(t, repo, "git", "push", "origin", "refs/tags/"+tag)
			}
			releaseCommand(t, repo, "deploy/publish-go-tag", "v0.5.5")
			releaseCommand(t, repo, "deploy/publish-go-tag", "v0.5.5")
			if got := releaseCommand(t, remote, "git", "rev-parse", tag+"^{commit}"); got != want {
				t.Fatalf("module tag = %s, want %s", got, want)
			}
			releaseCommand(t, repo, "git", "commit", "--allow-empty", "-m", "different source")
			cmd := exec.Command("deploy/publish-go-tag", "v0.5.5")
			cmd.Dir = repo
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "refusing to replace") {
				t.Fatalf("different source: %v\n%s", err, out)
			}
			if got := releaseCommand(t, remote, "git", "rev-parse", tag+"^{commit}"); got != want {
				t.Fatalf("published tag moved to %s", got)
			}
		})
	}
}
