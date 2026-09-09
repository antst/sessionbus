// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/antst/sessionbus/bus/internal/federation"
)

// addHost is an offline config edit. The hub deliberately reloads only on restart.
func addHost(args []string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	defaultConfig := os.Getenv("SESSIONBUS_HUB_CONFIG")
	if defaultConfig == "" {
		defaultConfig = filepath.Join(home, ".config/sessionbus/hub.json")
	}
	set := flag.NewFlagSet("sessionbus-hub add-host", flag.ContinueOnError)
	config := set.String("config", defaultConfig, "hub host map")
	secretFile := set.String("secret-file", "", "host's mode-0600 join secret file")
	if err := set.Parse(args); err != nil {
		return err
	}
	if set.NArg() != 1 || *secretFile == "" {
		return errors.New("usage: sessionbus-hub add-host -secret-file FILE [-config FILE] HOST")
	}
	info, err := os.Stat(*secretFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("join secret must be a mode-0600 regular file")
	}
	b, err := os.ReadFile(*secretFile)
	if err != nil {
		return err
	}
	secret, host := strings.TrimSpace(string(b)), set.Arg(0)
	resolved, err := filepath.EvalSymlinks(*config)
	if err != nil {
		return err
	}
	secrets, err := loadSecrets(resolved)
	if err != nil {
		return err
	}
	if old, ok := secrets[host]; ok {
		if old != secret {
			return fmt.Errorf("host %q already exists with a different secret; explicit rotation is required", host)
		}
		fmt.Println("Host already registered:", host)
		return nil
	}
	secrets[host] = secret
	if _, err := federation.ServerTLS(secrets); err != nil {
		return err
	}
	data, err := json.MarshalIndent(secrets, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(resolved), ".hub-hosts-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(append(data, '\n'))
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), resolved); err != nil {
		return err
	}
	fmt.Printf("Registered %s. Restart sessionbus-hub to load the updated host map.\n", host)
	return nil
}
