// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func TestAddHostPreservesOthersAndRefusesRotation(t *testing.T) {
	dir := t.TempDir()
	config, key := filepath.Join(dir, "hub.json"), filepath.Join(dir, "host.key")
	if err := os.WriteFile(config, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	secret, err := federation.NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte(secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-config", config, "-secret-file", key, "test-host"}
	if err := addHost(args); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := addHost(args); err != nil {
		t.Fatal("idempotent add", err)
	}
	if err := addHost([]string{"-config", config, "-secret-file", key, "another-host"}); err == nil {
		t.Fatal("duplicate secret accepted")
	}
	other, err := federation.NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte(other), 0600); err != nil {
		t.Fatal(err)
	}
	if err := addHost(args); err == nil {
		t.Fatal("implicit rotation accepted")
	}
	after, err := os.ReadFile(config)
	if err != nil || string(after) != string(before) {
		t.Fatal("failed operation changed config", err)
	}
	if err := addHost([]string{"-config", config, "-secret-file", key, "another-host"}); err != nil {
		t.Fatal(err)
	}
	hosts, err := loadSecrets(config)
	if err != nil || hosts["test-host"] != secret || hosts["another-host"] != other {
		t.Fatal("lost host", err)
	}
}
