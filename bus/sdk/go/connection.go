// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"net"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
)

// Connection is a schema-validated, full-duplex sessionbus connection.
type Connection = rpc.Conn

// Request is an inbound request with decoded, schema-validated Params.
type Request = rpc.Request

// NewConnection takes ownership of fd and immediately starts reading as a client.
// It does not dial, register, or reconnect. The non-nil handler runs in frame
// order on the reader; offload blocking work and calls that await responses.
// CallObserved callbacks also run on the reader, before the next frame is read.
// Close closes fd and cancels the context passed to the handler.
func NewConnection(fd net.Conn, handler func(context.Context, *Request)) *Connection {
	return rpc.New(fd, true, handler)
}
