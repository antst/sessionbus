// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

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

func TestAddHostWaitsForInterprocessLock(t *testing.T) {
	directory := t.TempDir()
	config, key := filepath.Join(directory, "hub.json"), filepath.Join(directory, "host.key")
	if err := os.WriteFile(config, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := federation.NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(key, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	held, err := lockHostConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = held.Close()
		}
	})
	command := exec.Command(os.Args[0], "-test.run=^TestHostConfigLockProcess$")
	command.Env = append(os.Environ(), "SESSIONBUS_TEST_HOST_LOCK="+config, "SESSIONBUS_TEST_HOST_KEY="+key)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 2)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	if line := <-lines; line != "attempting" {
		t.Fatalf("lock helper first line = %q", line)
	}
	select {
	case line := <-lines:
		t.Fatalf("lock acquired before release: %q", line)
	case <-time.After(50 * time.Millisecond):
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if line := <-lines; line != "completed" {
		t.Fatalf("lock helper second line = %q", line)
	}
	if err = command.Wait(); err != nil {
		t.Fatal(err)
	}
	hosts, err := loadSecrets(config)
	if err != nil || hosts["locked-host"] != secret {
		t.Fatalf("locked host update = %#v, %v", hosts, err)
	}
}

func TestHostConfigLockProcess(t *testing.T) {
	config := os.Getenv("SESSIONBUS_TEST_HOST_LOCK")
	if config == "" {
		return
	}
	fmt.Println("attempting")
	if err := addHostWithIO([]string{"-config", config, "-secret-file", os.Getenv("SESSIONBUS_TEST_HOST_KEY"), "locked-host"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	fmt.Println("completed")
}
