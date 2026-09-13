// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/antst/sessionbus/bus/internal/federation"
)

// addHost is an offline config edit. The hub deliberately reloads only on restart.
func addHost(args []string) error {
	return addHostWithIO(args, os.Stdout, os.Stderr)
}

func addHostWithIO(args []string, stdout, stderr io.Writer) (result error) {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		writeAddHostHelp(stdout)
		return nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	defaultConfig := os.Getenv("SESSIONBUS_HUB_CONFIG")
	if defaultConfig == "" {
		root := os.Getenv("XDG_CONFIG_HOME")
		if root == "" {
			root = filepath.Join(home, ".config")
		}
		defaultConfig = filepath.Join(root, "sessionbus/hub.json")
	}
	set := flag.NewFlagSet("sessionbus-hub add-host", flag.ContinueOnError)
	set.SetOutput(stderr)
	set.Usage = func() { writeAddHostHelp(stderr) }
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
	lock, err := lockHostConfig(resolved)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, lock.Close()) }()
	secrets, err := loadSecrets(resolved)
	if err != nil {
		return err
	}
	if old, ok := secrets[host]; ok {
		if old != secret {
			return fmt.Errorf("host %q already exists with a different secret; explicit rotation is required", host)
		}
		fmt.Fprintln(stdout, "Host already registered:", host)
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
	fmt.Fprintf(stdout, "Registered %s. Restart sessionbus-hub to load the updated host map.\n", host)
	return nil
}

type hostConfigLock struct{ file *os.File }

func lockHostConfig(config string) (*hostConfigLock, error) {
	file, err := os.OpenFile(config+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = file.Close()
		return nil, errors.New("hub config lock must be a mode-0600 regular file")
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &hostConfigLock{file: file}, nil
}

func (l *hostConfigLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
}
