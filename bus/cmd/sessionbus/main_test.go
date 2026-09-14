// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileSecretAndEnvironmentConfiguration(t *testing.T) {
	p := filepath.Join(t.TempDir(), "host.key")
	if err := os.WriteFile(p, []byte("test-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SESSIONBUS_HOST", "from-env")
	t.Setenv("SESSIONBUS_HUB", "hub:7419")
	t.Setenv("SESSIONBUS_HUB_SECRET_FILE", p)
	c, err := parse([]string{"-host", "explicit"})
	if err != nil || c.Host != "explicit" || c.HubAddress != "hub:7419" || c.HubSecret != "test-secret" {
		t.Fatal("configuration", err)
	}
	if _, err := parse([]string{"-hub-secret", "conflicting"}); err == nil {
		t.Fatal("ambiguous secret accepted")
	}
	if err := os.Chmod(p, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := parse(nil); err == nil {
		t.Fatal("public secret file accepted")
	}
	// A generated key alone must not turn a standalone host into a federated one.
	t.Setenv("SESSIONBUS_HUB", "")
	c, err = parse(nil)
	if err != nil || c.HubSecret != "" {
		t.Fatal("local host requires key", err)
	}
}

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

func TestProductsEnvironmentAndFlagPrecedence(t *testing.T) {
	t.Setenv("SESSIONBUS_HUB", "")
	t.Setenv("SESSIONBUS_PRODUCTS", "one-peer,two-peer")
	for _, row := range []struct {
		name string
		args []string
		want string
	}{
		{"environment", nil, "one-peer,two-peer"},
		{"override", []string{"-products", "three-peer"}, "three-peer"},
		{"clear", []string{"-products", ""}, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			c, err := parse(row.args)
			if err != nil || strings.Join(c.Products, ",") != row.want {
				t.Fatalf("products = %v, error = %v", c.Products, err)
			}
		})
	}
	t.Setenv("SESSIONBUS_PRODUCTS", "")
	c, err := parse(nil)
	if err != nil || len(c.Products) != 0 {
		t.Fatalf("empty environment: %v, %v", c.Products, err)
	}
}

func TestCommunicationLogConfiguration(t *testing.T) {
	t.Setenv("SESSIONBUS_HUB", "")
	t.Setenv("SESSIONBUS_COMMS_LOG", "")
	t.Setenv("SESSIONBUS_COMMS_LOG_DIR", "")
	c, err := parse(nil)
	if err != nil || c.CommsLog.Mode != "off" {
		t.Fatalf("default: %v %v", c.CommsLog.Mode, err)
	}
	t.Setenv("SESSIONBUS_COMMS_LOG", "content")
	t.Setenv("SESSIONBUS_COMMS_LOG_DIR", filepath.Join(t.TempDir(), "private"))
	c, err = parse(nil)
	if err != nil || c.CommsLog.Mode != "content" || c.CommsLog.Path != filepath.Join(os.Getenv("SESSIONBUS_COMMS_LOG_DIR"), "sessionbus.jsonl") || c.CommsLog.MaxFileBytes != 16<<20 || c.CommsLog.MaxFiles != 4 {
		t.Fatalf("configured: %#v %v", c.CommsLog, err)
	}
	c, err = parse([]string{"-comms-log", "metadata", "-comms-log-sessions", "one@host,two@host", "-comms-log-groups", "team"})
	if err != nil || c.CommsLog.Mode != "metadata" || len(c.CommsLog.Sessions) != 2 || len(c.CommsLog.Groups) != 1 {
		t.Fatalf("override: %#v %v", c.CommsLog, err)
	}
	if _, err = parse([]string{"-comms-log", "typo"}); err == nil {
		t.Fatal("invalid mode accepted")
	}
}
