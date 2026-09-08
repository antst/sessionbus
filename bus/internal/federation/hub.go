// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type Hub struct {
	listener net.Listener
	tls      *tls.Config
	registry *Registry
	accepted chan struct{}
}

type Registry struct {
	ctx    context.Context
	cancel context.CancelFunc
	inbox  chan registryEvent
	done   chan struct{}
	log    io.Writer
}

type registryEventKind uint8

const (
	registryAccept registryEventKind = iota + 1
	registryRetire
	registryReady
	registryClosed
	registryHosts
	registryForward
	registryReply
)

type registryEvent struct {
	kind    registryEventKind
	link    *hostLink
	fd      net.Conn
	tls     *tls.Config
	forward *incomingForward
	reply   *originReply
	id      int64
	ack     chan bool
}

type hostLink struct {
	ctx      context.Context
	host     string
	fd, raw  net.Conn
	wire     *conn.Conn
	inbox    chan any
	registry chan<- registryEvent
	done     chan struct{}
	group    sync.WaitGroup
	log      io.Writer
}

type hubState struct {
	links   map[string]*hostLink
	sockets map[net.Conn]bool
	group   sync.WaitGroup
	log     io.Writer
}

type outgoingForward struct {
	value Forward
	reply chan Reply
}

type incomingForward struct {
	id    int64
	value Forward
	host  string
}

type originReply struct {
	origin *hostLink
	id     int64
	value  Reply
}

func StartHub(listener net.Listener, configuration *tls.Config, stderr io.Writer) *Hub {
	h := &Hub{listener: listener, tls: configuration, registry: NewRegistry(stderr), accepted: make(chan struct{})}
	go h.accept()
	return h
}

func (h *Hub) accept() {
	defer close(h.accepted)
	for {
		fd, err := h.listener.Accept()
		if err != nil || !h.registry.Accept(fd, h.tls) {
			return
		}
	}
}

func (h *Hub) Close() {
	_ = h.listener.Close()
	<-h.accepted
	h.registry.Close()
}

func NewRegistry(stderr io.Writer) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{ctx: ctx, cancel: cancel, inbox: make(chan registryEvent, conn.OutboxSize), done: make(chan struct{}), log: stderr}
	go r.run()
	return r
}

func (r *Registry) Accept(fd net.Conn, configuration *tls.Config) bool {
	ack := make(chan bool, 1)
	if !postRegistry(r.ctx, r.inbox, registryEvent{kind: registryAccept, fd: fd, tls: configuration, ack: ack}) {
		_ = fd.Close()
		return false
	}
	select {
	case <-ack:
		return true
	case <-r.ctx.Done():
		_ = fd.Close()
		return false
	}
}

func (r *Registry) Close() {
	r.cancel()
	<-r.done
}

func (r *Registry) run() {
	state := hubState{links: map[string]*hostLink{}, sockets: map[net.Conn]bool{}, log: r.log}
	defer close(r.done)
	for {
		select {
		case event := <-r.inbox:
			state.handle(r.ctx, r.inbox, event)
		case <-r.ctx.Done():
			for fd := range state.sockets {
				_ = fd.Close()
			}
			for _, link := range state.links {
				link.wire.Close()
			}
			state.group.Wait()
			return
		}
	}
}

func (s *hubState) handle(ctx context.Context, inbox chan<- registryEvent, event registryEvent) {
	switch event.kind {
	case registryAccept:
		s.sockets[event.fd] = true
		s.group.Add(1)
		go func() { defer s.group.Done(); authenticate(ctx, event.fd, event.tls, inbox, s.log) }()
		event.ack <- true
	case registryRetire:
		delete(s.sockets, event.fd)
	case registryReady:
		delete(s.sockets, event.link.raw)
		accepted := s.links[event.link.host] == nil
		if accepted {
			s.links[event.link.host] = event.link
		} else {
			fmt.Fprintf(s.log, "duplicate federation host %q\n", event.link.host)
			event.link.wire.Close()
		}
		event.ack <- accepted
	case registryClosed:
		if s.links[event.link.host] == event.link {
			delete(s.links, event.link.host)
		}
		event.ack <- true
	case registryHosts:
		if s.links[event.link.host] == event.link {
			hosts := make([]string, 0, len(s.links)-1)
			for host := range s.links {
				if host != event.link.host {
					hosts = append(hosts, host)
				}
			}
			sort.Strings(hosts)
			raw, _ := json.Marshal(hostsResult{Hosts: hosts})
			s.reply(originReply{event.link, event.id, Reply{Result: raw}})
		}
	case registryForward:
		s.forward(ctx, inbox, event.link, *event.forward)
	case registryReply:
		s.reply(*event.reply)
	}
}

func authenticate(ctx context.Context, raw net.Conn, configuration *tls.Config, registry chan<- registryEvent, log io.Writer) {
	fd := tls.Server(raw, configuration)
	if err := fd.HandshakeContext(ctx); err != nil {
		fmt.Fprintln(log, err)
		_ = raw.Close()
		postRegistry(ctx, registry, registryEvent{kind: registryRetire, fd: raw})
		return
	}
	link := newHostLink(ctx, fd.ConnectionState().ServerName, fd, raw, registry, log)
	ack := make(chan bool, 1)
	accepted := false
	if postRegistry(ctx, registry, registryEvent{kind: registryReady, link: link, ack: ack}) {
		select {
		case accepted = <-ack:
		case <-ctx.Done():
		}
	}
	if !accepted {
		link.wire.Close()
	}
	link.run(accepted)
}

func newHostLink(ctx context.Context, host string, fd, raw net.Conn, registry chan<- registryEvent, log io.Writer) *hostLink {
	link := &hostLink{ctx: ctx, host: host, fd: fd, raw: raw, inbox: make(chan any, conn.OutboxSize), registry: registry, done: make(chan struct{}), log: log}
	link.wire = conn.Start(fd, link.inbox, &link.group)
	return link
}

func postRegistry(ctx context.Context, target chan<- registryEvent, event registryEvent) bool {
	select {
	case target <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *hostLink) run(accepted bool) {
	defer func() {
		l.wire.Close()
		l.group.Wait()
		close(l.done)
	}()
	failed, nextID, lastIn := !accepted, int64(0), int64(0)
	stopping := l.ctx.Done()
	pending := map[int64]pendingCall{}
	for {
		select {
		case <-stopping:
			l.wire.Close()
			failed, stopping = true, nil
		case raw := <-l.inbox:
			switch event := raw.(type) {
			case outgoingForward:
				if failed || len(pending) >= maxPendingForwardedPerHost {
					event.reply <- errorReply(protocol.ForwardLost, nil)
					if len(pending) >= maxPendingForwardedPerHost {
						l.wire.Close()
						failed = true
					}
					continue
				}
				nextID++
				body, err := requestBytes(nextID, forwardMethod, event.value)
				if err != nil {
					event.reply <- errorReply(protocol.InvalidFrame, "forwarded request exceeds the frame limit")
					continue
				}
				if !l.wire.Send(body) {
					event.reply <- errorReply(protocol.ForwardLost, nil)
					failed = true
					continue
				}
				pending[nextID] = pendingCall{event.value.Request.Method, event.reply}
			case originReply:
				body, err := ResponseBytes(event.id, event.value)
				if !failed && (err != nil || !l.wire.Send(body)) {
					failed = true
					l.wire.Close()
				}
			case conn.Frame:
				if !failed {
					if err := l.frame(event, pending, &lastIn); err != nil {
						fmt.Fprintln(l.log, err)
						failed = true
						l.wire.Close()
					}
				}
			case conn.Closed:
				ack := make(chan bool, 1)
				if postRegistry(l.ctx, l.registry, registryEvent{kind: registryClosed, link: l, ack: ack}) {
					select {
					case <-ack:
					case <-l.ctx.Done():
					}
				}
				settlePending(pending)
				l.settleQueued()
				return
			}
		}
	}
}

func (l *hostLink) settleQueued() {
	for {
		select {
		case value := <-l.inbox:
			if call, ok := value.(outgoingForward); ok {
				call.reply <- errorReply(protocol.ForwardLost, nil)
			}
		default:
			return
		}
	}
}

func (l *hostLink) frame(event conn.Frame, pending map[int64]pendingCall, lastIn *int64) error {
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
	*lastIn = frame.ID
	if frame.Method == hostsMethod {
		var params map[string]json.RawMessage
		if protocol.DecodeJSON(frame.Params, &params) != nil || params == nil || len(params) != 0 {
			return errFrame
		}
		postRegistry(l.ctx, l.registry, registryEvent{kind: registryHosts, link: l, id: frame.ID})
		return nil
	}
	if frame.Method != forwardMethod {
		return errFrame
	}
	value, host, err := decodeForward(frame.Params, l.host)
	if err == nil {
		postRegistry(l.ctx, l.registry, registryEvent{kind: registryForward, link: l, forward: &incomingForward{id: frame.ID, value: value, host: host}})
	}
	return err
}

func settlePending(pending map[int64]pendingCall) {
	for id, call := range pending {
		delete(pending, id)
		call.reply <- errorReply(protocol.ForwardLost, nil)
	}
}

func validReply(method string, frame protocol.Frame) bool {
	if frame.Error != nil {
		return true
	}
	if method == hostsMethod {
		_, err := DecodeHosts(Reply{Result: frame.Result})
		return err == nil
	}
	_, err := protocol.DecodeResult(method, frame.Result)
	return err == nil
}

func (s *hubState) forward(ctx context.Context, inbox chan<- registryEvent, origin *hostLink, call incomingForward) {
	if s.links[origin.host] != origin {
		return
	}
	destination := s.links[call.host]
	if destination == nil {
		s.reply(originReply{origin, call.id, errorReply(protocol.UnknownHost, nil)})
		return
	}
	reply := make(chan Reply, 1)
	if !destination.wire.Post(outgoingForward{call.value, reply}) {
		reply <- errorReply(protocol.ForwardLost, nil)
		s.remove(destination)
	}
	s.group.Add(1)
	go func() {
		defer s.group.Done()
		select {
		case value := <-reply:
			postRegistry(ctx, inbox, registryEvent{kind: registryReply, reply: &originReply{origin, call.id, value}})
		case <-origin.done:
		case <-ctx.Done():
		}
	}()
}

func (s *hubState) reply(event originReply) {
	if s.links[event.origin.host] != event.origin || !event.origin.wire.Post(event) {
		s.remove(event.origin)
	}
}

func (s *hubState) remove(link *hostLink) {
	if link != nil && s.links[link.host] == link {
		delete(s.links, link.host)
		link.wire.Close()
	}
}
