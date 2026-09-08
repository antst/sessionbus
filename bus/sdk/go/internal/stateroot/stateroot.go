// SPDX-License-Identifier: MIT

package stateroot

import (
	"os"
	"path/filepath"
	"strconv"
)

// SessionSocket resolves the client-side socket discovery order.
func SessionSocket() (string, error) {
	if socket := os.Getenv("SESSIONBUS_SOCKET"); socket != "" {
		return filepath.Abs(socket)
	}
	root, err := RuntimeRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "presence.sock"), nil
}

// RuntimeRoot returns the short-lived Sessionbus socket directory.
func RuntimeRoot() (string, error) {
	root := os.Getenv("XDG_RUNTIME_DIR")
	if root != "" {
		root = filepath.Join(root, "sessionbus")
	} else {
		root = filepath.Join("/tmp", "sessionbus-"+strconv.Itoa(os.Getuid()))
	}
	return filepath.Abs(root)
}
