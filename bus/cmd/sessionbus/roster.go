// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/antst/sessionbus/bus/internal/roster"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func runRoster(arguments []string, output io.Writer) error {
	set := flag.NewFlagSet("sessionbus roster", flag.ContinueOnError)
	set.SetOutput(output)
	asJSON := set.Bool("json", false, "emit sessionbus.roster.v1 JSON")
	local := set.Bool("local", false, "inspect this host only")
	socket := set.String("socket", sessionkit.Socket(), "daemon's public Unix socket path")
	timeout := set.Duration("timeout", 15*time.Second, "total roster deadline")
	if err := set.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if set.NArg() != 0 {
		return errors.New("roster received unexpected arguments")
	}
	if *timeout <= 0 {
		return errors.New("roster timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	fd, err := (&net.Dialer{}).DialContext(ctx, "unix", roster.Socket(*socket))
	if err != nil {
		return fmt.Errorf("connect operator roster (daemon must support v0.5.1 or later): %w", err)
	}
	defer fd.Close()
	deadline, _ := ctx.Deadline()
	_ = fd.SetDeadline(deadline)
	if err = json.NewEncoder(fd).Encode(roster.Request{Local: *local}); err != nil {
		return err
	}
	body, err := bufio.NewReaderSize(fd, protocol.MaxFrameBytes).ReadSlice('\n')
	if err != nil {
		return err
	}
	var value roster.Report
	if err = protocol.DecodeJSON(body, &value); err != nil || value.Schema != roster.Schema {
		return errors.New("invalid operator roster response")
	}
	if *asJSON {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(value)
	} else {
		err = renderRoster(output, value)
	}
	if err != nil {
		return err
	}
	if !value.Complete {
		return errors.New("operator roster is incomplete; see host errors (use --local for local-only inspection)")
	}
	return nil
}

func renderRoster(output io.Writer, value roster.Report) error {
	writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	fmt.Fprintf(writer, "Host: %s   Federation: %s\n", safeCell(value.Local.Host), safeCell(value.Federation))
	fmt.Fprintln(writer, "HOST\tKIND\tSTATE\tPRODUCT\tNAME\tSESSION ID\tGROUPS\tOWNER\tPERMISSION\tPERSISTENT")
	hosts := append([]roster.Host{value.Local}, value.Remote...)
	for _, host := range hosts {
		if host.Error != "" {
			fmt.Fprintf(writer, "%s\tERROR\t%s\n", safeCell(host.Host), safeCell(host.Error))
			continue
		}
		if len(host.Sessions) == 0 {
			fmt.Fprintf(writer, "%s\t(no sessions)\n", safeCell(host.Host))
		}
		for _, row := range host.Sessions {
			state := "disconnected"
			if row.Connected {
				state = "idle"
			}
			if row.Running {
				state = "running"
			}
			fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%t\n", safeCell(host.Host), safeCell(row.Kind), state, safeCell(row.Product), safeCell(row.Name), safeCell(row.SessionID), safeCell(strings.Join(row.Groups, ",")), safeCell(row.Owner), safeCell(row.PermissionMode), row.Persistent)
		}
	}
	if value.Error != "" {
		fmt.Fprintf(writer, "ERROR: %s\n", safeCell(value.Error))
	}
	return writer.Flush()
}

func safeCell(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
}
