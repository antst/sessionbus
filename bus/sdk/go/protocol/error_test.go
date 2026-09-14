// SPDX-License-Identifier: MIT

package protocol

import (
	"bytes"
	"testing"
)

func TestUnsupportedTraceErrorEncoding(t *testing.T) {
	raw, err := ErrorBytes(7, UnsupportedTrace, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if frame.ID != 7 || frame.Error == nil || frame.Error.Code != UnsupportedTrace || frame.Error.Message != "unsupported_trace" || frame.Error.Data != nil {
		t.Fatalf("frame = %#v", frame)
	}
	if bytes.Contains(raw, []byte(`"data"`)) {
		t.Fatalf("unsupported_trace carried data: %s", raw)
	}
	if _, err := ErrorBytes(7, UnsupportedTrace, "upgrade"); err == nil {
		t.Fatal("unsupported_trace accepted data")
	}
}
