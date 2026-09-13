// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"encoding/json"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const ownerEndMethod = "federation.owner_end"
const hostEndMethod = "federation.host_end"

type OwnerEnd struct {
	Host       string `json:"host"`
	SessionID  string `json:"owner_session_id"`
	Lifetime   string `json:"owner_lifetime"`
	Source     string `json:"source,omitempty"`
	Attachment string `json:"source_attachment,omitempty"`
}
type HostEnd struct {
	Host       string `json:"host"`
	Attachment string `json:"source_attachment"`
}
type LifetimeEvent struct {
	Owner *OwnerEnd
	Host  *HostEnd
}
type OwnerEndCall struct {
	Value OwnerEnd
	Reply chan Reply
}
type lifetimeCall struct {
	Method string
	Value  any
	Reply  chan Reply
}
type incomingOwnerEnd struct {
	ID    int64
	Value OwnerEnd
}

func emptyReply() Reply { return Reply{Result: json.RawMessage(`{}`)} }
func validControlReply(frame protocol.Frame) bool {
	var value map[string]json.RawMessage
	return frame.Error != nil || protocol.DecodeJSON(frame.Result, &value) == nil && value != nil && len(value) == 0
}
