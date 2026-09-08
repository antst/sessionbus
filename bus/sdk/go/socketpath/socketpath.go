// SPDX-License-Identifier: MIT

// Package socketpath defines the fixed Sessionbus lane-socket mapping.
package socketpath

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

const laneHashBytes = 16

// Lane returns the fixed-length socket path for one qualified session ID.
func Lane(socket, sessionID string) string {
	digest := sha256.Sum256([]byte(sessionID))
	return filepath.Join(filepath.Dir(socket), "lanes", hex.EncodeToString(digest[:laneHashBytes])+".sock")
}
