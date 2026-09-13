// SPDX-License-Identifier: GPL-3.0-only

package roster

import (
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/socketpath"
)

func TestOperatorEndpointFitsWhereLanesFit(t *testing.T) {
	for _, socket := range []string{"/run/user/1000/sessionbus/presence.sock", "/tmp/with spaces/custom.sock"} {
		if len(Socket(socket)) > len(socketpath.Lane(socket, "")) {
			t.Fatal("operator endpoint is longer than mandatory lane endpoint")
		}
		if Socket(socket) == Socket(socket+"-other") {
			t.Fatal("operator collision")
		}
	}
}

func TestRosterDecodeRejectsPayloadAndDuplicateHost(t *testing.T) {
	valid := `{"hosts":[{"host":"alpha","products":[],"sessions":[]}]}`
	if _, err := DecodeRemote([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{}`, `{"hosts":null}`, `{"hosts":[{"host":"alpha","products":[],"sessions":[],"secret":"bad"}]}`, `{"hosts":[{"host":"alpha","products":[],"sessions":[]},{"host":"alpha","products":[],"sessions":[]}]}`, strings.Repeat("x", MaxBytes+1)} {
		if _, err := DecodeRemote([]byte(body)); err == nil {
			t.Fatal("accepted invalid roster")
		}
	}
}
