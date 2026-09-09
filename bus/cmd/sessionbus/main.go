// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/antst/sessionbus/bus/internal/daemon"
	"github.com/antst/sessionbus/bus/internal/federation"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	return runTo(arguments, os.Stdout)
}

func runTo(arguments []string, stdout io.Writer) error {
	if len(arguments) == 1 && arguments[0] == "secret" {
		secret, err := federation.NewSecret()
		if err == nil {
			_, err = fmt.Fprintln(stdout, secret)
		}
		return err
	}
	configuration, err := parse(arguments)
	if err != nil {
		return err
	}
	service, err := daemon.Start(configuration)
	if err != nil {
		return err
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	select {
	case <-interrupt:
	case <-service.Done():
	}
	return service.Close()
}

func parse(arguments []string) (daemon.Config, error) {
	root, err := stateRoot()
	if err != nil {
		return daemon.Config{}, err
	}
	socket := sessionkit.Socket()
	set := flag.NewFlagSet("sessionbus", flag.ContinueOnError)
	configuration := daemon.Config{}
	products := ""
	set.StringVar(&configuration.SocketPath, "socket", socket, "unix socket path")
	set.StringVar(&configuration.TablePath, "table", filepath.Join(root, "sessions.json"), "durable session table")
	set.StringVar(&configuration.Host, "host", os.Getenv("SESSIONBUS_HOST"), "local host name")
	set.StringVar(&products, "products", "", "comma-separated advertised products")
	set.StringVar(&configuration.HubAddress, "hub", os.Getenv("SESSIONBUS_HUB"), "federation hub address")
	set.StringVar(&configuration.HubSecret, "hub-secret", "", "federation host secret")
	var secretFile string
	set.StringVar(&secretFile, "hub-secret-file", os.Getenv("SESSIONBUS_HUB_SECRET_FILE"), "mode-0600 federation secret file (used when connecting to a hub)")
	if err := set.Parse(arguments); err != nil {
		return daemon.Config{}, err
	}
	if set.NArg() != 0 {
		return daemon.Config{}, fmt.Errorf("unexpected argument %q", set.Arg(0))
	}
	if secretFile != "" && configuration.HubAddress != "" {
		if configuration.HubSecret != "" {
			return daemon.Config{}, fmt.Errorf("use only one of -hub-secret and -hub-secret-file")
		}
		info, err := os.Stat(secretFile)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return daemon.Config{}, fmt.Errorf("hub secret file must be a mode-0600 regular file")
		}
		b, err := os.ReadFile(secretFile)
		if err != nil {
			return daemon.Config{}, err
		}
		configuration.HubSecret = strings.TrimSpace(string(b))
	}
	if products != "" {
		configuration.Products = strings.Split(products, ",")
	}
	return configuration, nil
}

func stateRoot() (string, error) {
	if root := os.Getenv("XDG_STATE_HOME"); root != "" {
		return filepath.Abs(filepath.Join(root, "sessionbus"))
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(home, ".local", "state", "sessionbus"))
}
