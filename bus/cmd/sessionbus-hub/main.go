// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) > 0 && arguments[0] == "add-host" {
		return addHost(arguments[1:])
	}
	set := flag.NewFlagSet("sessionbus-hub", flag.ContinueOnError)
	listen, path := "", ""
	set.StringVar(&listen, "listen", os.Getenv("SESSIONBUS_HUB_LISTEN"), "TCP listen address")
	set.StringVar(&path, "config", os.Getenv("SESSIONBUS_HUB_CONFIG"), "mode-0600 host secret map")
	if err := set.Parse(arguments); err != nil {
		return err
	}
	if set.NArg() != 0 || listen == "" || path == "" {
		return errors.New("-listen and -config are required")
	}
	secrets, err := loadSecrets(path)
	if err != nil {
		return err
	}
	configuration, err := federation.ServerTLS(secrets)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	hub := federation.StartHub(listener, configuration, os.Stderr)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interrupt
	signal.Stop(interrupt)
	hub.Close()
	return nil
}

func loadSecrets(path string) (map[string]string, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	info, err := fd.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("federation config must be a mode-0600 regular file")
	}
	decoder := json.NewDecoder(fd)
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("federation config must be a JSON object")
	}
	secrets := map[string]string{}
	for decoder.More() {
		token, tokenErr := decoder.Token()
		host, ok := token.(string)
		_, exists := secrets[host]
		if tokenErr != nil || !ok || exists {
			return nil, errors.New("invalid or duplicate federation host")
		}
		var secret string
		if decoder.Decode(&secret) != nil {
			return nil, errors.New("federation secret must be a string")
		}
		secrets[host] = secret
	}
	if _, err = decoder.Token(); err != nil {
		return nil, err
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("trailing federation config value")
	}
	return secrets, nil
}
