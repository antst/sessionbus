// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseLocalConfiguration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	socket := filepath.Join(t.TempDir(), "sessionbus.sock")
	table := filepath.Join(t.TempDir(), "sessions.json")
	configuration, err := parse([]string{"-socket", socket, "-table", table, "-host", "test-host", "-products", "one-peer,two-peer", "-hub", "hub:9443", "-hub-secret", "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if configuration.SocketPath != socket || configuration.TablePath != table || configuration.Host != "test-host" || len(configuration.Products) != 2 || configuration.HubAddress != "hub:9443" || configuration.HubSecret != "secret" {
		t.Fatalf("configuration = %#v", configuration)
	}
}

func TestSecretCommand(t *testing.T) {
	var output bytes.Buffer
	if err := runTo([]string{"secret"}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(output.String(), "\n") || strings.Count(output.String(), "\n") != 1 {
		t.Fatalf("output = %q", output.String())
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSuffix(output.String(), "\n"))
	if err != nil || len(decoded) != 32 {
		t.Fatalf("secret length = %d, error = %v", len(decoded), err)
	}
}
