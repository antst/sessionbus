// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type IncomingCall struct {
	From    Caller
	Request PublicRequest
}

type Wait func(<-chan struct{}) (Reply, bool)

type OutgoingCall struct {
	Value Forward
	Reply chan Reply
}

type HostsCall struct{ Reply chan Reply }

type daemonReply struct {
	id    int64
	value Reply
}

type pendingCall struct {
	method string
	reply  chan Reply
}

func ServeDaemon(ctx context.Context, host string, fd net.Conn, inbox chan any, admit func(IncomingCall) (Wait, error), stderr io.Writer, lifetime ...func(LifetimeEvent)) error {
	var group, helpers sync.WaitGroup
	wire := conn.Start(fd, inbox, &group)
	nextID, lastIn := int64(0), int64(0)
	pending := map[int64]pendingCall{}
	stopping := ctx.Done()
	var cause error
	defer func() {
		wire.Close()
		group.Wait()
		helpers.Wait()
	}()
	for {
		select {
		case <-stopping:
			wire.Close()
			stopping, cause = nil, ctx.Err()
		case raw := <-inbox:
			switch event := raw.(type) {
			case OwnerEndCall:
				if len(pending) >= protocol.MaxOperations {
					event.Reply <- errorReply(protocol.ForwardLost, nil)
					wire.Close()
					continue
				}
				nextID++
				body, err := requestBytes(nextID, ownerEndMethod, event.Value)
				if cause != nil || err != nil || !wire.Send(body) {
					event.Reply <- errorReply(protocol.ForwardLost, nil)
					wire.Close()
					continue
				}
				pending[nextID] = pendingCall{ownerEndMethod, event.Reply}
			case OutgoingCall:
				nextID++
				body, err := requestBytes(nextID, forwardMethod, event.Value)
				if cause != nil || err != nil || !wire.Send(body) {
					code, data := protocol.ForwardLost, any(nil)
					if err != nil {
						code, data = protocol.InvalidFrame, "forwarded request exceeds the frame limit"
					}
					event.Reply <- errorReply(code, data)
					if err == nil {
						wire.Close()
						cause = errFrame
					}
					continue
				}
				pending[nextID] = pendingCall{event.Value.Request.Method, event.Reply}
			case HostsCall:
				nextID++
				body, err := requestBytes(nextID, hostsMethod, struct{}{})
				if cause != nil || err != nil || !wire.Send(body) {
					event.Reply <- errorReply(protocol.ForwardLost, nil)
					wire.Close()
					cause = errFrame
					continue
				}
				pending[nextID] = pendingCall{hostsMethod, event.Reply}
			case daemonReply:
				body, err := ResponseBytes(event.id, event.value)
				if cause == nil && (err != nil || !wire.Send(body)) {
					cause = errFrame
					wire.Close()
				}
			case conn.Frame:
				if cause == nil {
					cause = daemonFrame(host, event, pending, wire, inbox, admit, &helpers, &lastIn, lifetime)
					if cause != nil {
						fmt.Fprintln(stderr, cause)
						wire.Close()
					}
				}
			case conn.Closed:
				settlePending(pending)
				if cause != nil {
					return cause
				}
				return event.Cause
			}
		}
	}
}

func daemonFrame(host string, event conn.Frame, pending map[int64]pendingCall, wire *conn.Conn, inbox chan any, admit func(IncomingCall) (Wait, error), helpers *sync.WaitGroup, lastIn *int64, lifetime []func(LifetimeEvent)) error {
	frame := event.Value
	if event.Err != nil {
		return event.Err
	}
	if !frame.Request {
		call, ok := pending[frame.ID]
		if !ok || !validReply(call.method, frame) {
			return errFrame
		}
		delete(pending, frame.ID)
		call.reply <- Reply{Result: frame.Result, Error: frame.Error, ErrorRaw: frame.ErrorRaw}
		return nil
	}
	if frame.ID <= *lastIn {
		return errFrame
	}
	if frame.Method == ownerEndMethod || frame.Method == hostEndMethod {
		*lastIn = frame.ID
		event := LifetimeEvent{}
		if frame.Method == ownerEndMethod {
			var value OwnerEnd
			if protocol.DecodeJSON(frame.Params, &value) != nil || value.Host != host || !validHost(value.Source) || !ownedPart(value.SessionID, value.Source, false) || value.Lifetime == "" || value.Attachment == "" {
				return errFrame
			}
			event.Owner = &value
		} else {
			var value HostEnd
			if protocol.DecodeJSON(frame.Params, &value) != nil || !validHost(value.Host) || value.Attachment == "" {
				return errFrame
			}
			event.Host = &value
		}
		for _, apply := range lifetime {
			apply(event)
		}
		body, _ := ResponseBytes(frame.ID, emptyReply())
		if !wire.Send(body) {
			return errFrame
		}
		return nil
	}
	if frame.Method != forwardMethod || frame.ID <= *lastIn {
		return errFrame
	}
	*lastIn = frame.ID
	source, err := sourceHost(frame.Params)
	if err != nil {
		return err
	}
	value, target, err := decodeForward(frame.Params, source)
	if err != nil || target != host {
		return errFrame
	}
	wait, err := admit(IncomingCall{From: value.From, Request: value.Request})
	if err != nil {
		return err
	}
	helpers.Add(1)
	go func(id int64) {
		defer helpers.Done()
		if value, ok := wait(wire.Done()); ok {
			if !wire.Post(daemonReply{id, value}) {
				wire.Close()
			}
		}
	}(frame.ID)
	return nil
}

func DecodeHosts(reply Reply) ([]string, error) {
	if reply.Error != nil {
		return nil, errFrame
	}
	var value hostsResult
	if protocol.DecodeJSON(reply.Result, &value) != nil || value.Hosts == nil || !uniqueStrings(value.Hosts) {
		return nil, errFrame
	}
	for _, host := range value.Hosts {
		if !validHost(host) {
			return nil, errFrame
		}
	}
	return value.Hosts, nil
}

func sourceHost(raw []byte) (string, error) {
	var value struct {
		From struct {
			SessionID string `json:"session_id"`
		} `json:"from"`
	}
	if jsonErr := json.Unmarshal(raw, &value); jsonErr != nil {
		return "", errFrame
	}
	return suffix(value.From.SessionID)
}
