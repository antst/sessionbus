package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func TestReviewerAddHostUsesInstallerXDGPath(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(home, "custom-config")
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("SESSIONBUS_HUB_CONFIG", "")
	path := filepath.Join(root, "sessionbus", "hub.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(home, "host.key")
	secret, err := federation.NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	if err := addHost([]string{"-secret-file", key, "workstation"}); err != nil {
		t.Fatal(err)
	}
	hosts, err := loadSecrets(path)
	if err != nil || hosts["workstation"] != secret {
		t.Fatalf("wrong installed map: %v", err)
	}
}
