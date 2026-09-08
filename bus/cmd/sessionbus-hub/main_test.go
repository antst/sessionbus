// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func TestLoadSecretsIsStrictAndMode0600(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString(make([]byte, federation.SecretBytes))
	path := filepath.Join(t.TempDir(), "hub.json")
	write := func(body string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"alpha":"`+secret+`"}`, 0o600)
	if value, err := loadSecrets(path); err != nil || value["alpha"] != secret {
		t.Fatalf("config = %#v, %v", value, err)
	}
	for _, body := range []string{
		`{"alpha":"` + secret + `","alpha":"` + secret + `"}`,
		`{"alpha":1}`,
		`{"alpha":"` + secret + `"} {}`,
	} {
		write(body, 0o600)
		if _, err := loadSecrets(path); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	write(`{"alpha":"`+secret+`"}`, 0o644)
	if _, err := loadSecrets(path); err == nil {
		t.Fatal("accepted readable federation config")
	}
}

func TestHubUsesEnvironmentWhenFlagsAreAbsent(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	t.Setenv("SESSIONBUS_HUB_LISTEN", "127.0.0.1:0")
	t.Setenv("SESSIONBUS_HUB_CONFIG", missing)
	if err := run(nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("environment defaults error = %v", err)
	}
}
