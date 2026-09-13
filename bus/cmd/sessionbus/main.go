// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"errors"
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
	if len(arguments) > 0 && arguments[0] == "help" {
		switch {
		case len(arguments) == 1:
			arguments = []string{"--help"}
		case len(arguments) == 2 && arguments[1] == "roster":
			arguments = []string{"roster", "--help"}
		case len(arguments) == 2 && arguments[1] == "secret":
			arguments = []string{"secret", "--help"}
		default:
			return errors.New("usage: sessionbus help [roster|secret]")
		}
	}
	if len(arguments) == 2 && arguments[0] == "secret" && (arguments[1] == "--help" || arguments[1] == "-h") {
		_, err := fmt.Fprintln(stdout, "Usage: sessionbus secret\n\nGenerate a fresh base64 federation join secret on stdout. This command does not\ninstall or rotate any key. Keep the output private; host installation normally\ngenerates and preserves ${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/host.key.")
		return err
	}
	if len(arguments) > 0 && arguments[0] == "roster" {
		return runRoster(arguments[1:], stdout)
	}
	if len(arguments) == 1 && arguments[0] == "secret" {
		secret, err := federation.NewSecret()
		if err == nil {
			_, err = fmt.Fprintln(stdout, secret)
		}
		return err
	}
	configuration, err := parseTo(arguments, stdout)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
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

func parse(arguments []string) (daemon.Config, error) { return parseTo(arguments, os.Stderr) }

func parseTo(arguments []string, output io.Writer) (daemon.Config, error) {
	root, err := stateRoot()
	if err != nil {
		return daemon.Config{}, err
	}
	socket := sessionkit.Socket()
	set := flag.NewFlagSet("sessionbus", flag.ContinueOnError)
	set.SetOutput(output)
	set.Usage = func() { fmt.Fprint(output, daemonHelp); set.PrintDefaults() }
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

const daemonHelp = `Sessionbus — local and federated session router

Usage:
  sessionbus [daemon flags]           Run the daemon in the foreground
  sessionbus roster [--all] [--json] [--local]  Inspect online peers and active lanes
  sessionbus secret                  Generate a private federation join secret
  sessionbus help [roster|secret]     Show command help

Examples:
  sessionbus roster
  sessionbus roster --json
  sessionbus roster --all            Include offline and archived entries
  sessionbus roster --local --socket /path/to/presence.sock

The binary has no implicit install, start, or restart subcommand. Installed
services are managed by systemd --user (Linux) or launchd (macOS):
  systemctl --user status sessionbus
  systemctl --user restart sessionbus
  launchctl kickstart -k gui/$(id -u)/net.antst.sessionbus

Socket discovery: SESSIONBUS_SOCKET, then $XDG_RUNTIME_DIR/sessionbus/presence.sock,
or /tmp/sessionbus-<uid>/presence.sock. State: $XDG_STATE_HOME/sessionbus,
or ~/.local/state/sessionbus. Federation flags default from SESSIONBUS_HOST,
SESSIONBUS_HUB, and SESSIONBUS_HUB_SECRET_FILE. The installer preserves those
settings in ${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/service.env; the service
manager loads that file.
Prefer -hub-secret-file over placing a secret in process arguments.
Hub outages do not stop local service: the daemon retries its connection with
bounded backoff. Lost remote operations fail without automatic replay.

Daemon flags:
`
