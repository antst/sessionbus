// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type entry struct {
	lifetime       *ownership
	parent         *ownership
	row            row
	peer           bool
	info           map[string]any
	declaredGroups []string
	claimed        bool
	running        bool
	attachment     *session
	done           chan struct{}
}

type directory struct {
	remoteOwners map[string]*ownership
	daemon       *Daemon
	mu           sync.Mutex
	closing      bool
	entries      map[string]*entry
	names        map[string]*entry
	tokens       map[string]*launch
}

type selected struct {
	label     string
	item      *entry
	summary   protocol.SessionSummary
	code      int
	ambiguous bool
}

func newDirectory(daemon *Daemon, rows []row) *directory {
	d := &directory{daemon: daemon, entries: map[string]*entry{}, names: map[string]*entry{}, tokens: map[string]*launch{}, remoteOwners: map[string]*ownership{}}
	for _, value := range rows {
		item := &entry{row: cloneRow(value), done: closedChannel()}
		d.entries[value.SessionID], d.names[value.Name] = item, item
	}
	return d
}

func (d *directory) installPeer(owner *session, hello *protocol.PeerHello, host string) (current *entry, displaced *session, ended *entry, ok bool) {
	id, name := qualify(hello.SessionID, host), qualify(hello.Name, host)
	d.mu.Lock()
	defer d.mu.Unlock()
	old := owner.identity
	if old != nil && old.attachment != owner {
		return nil, nil, nil, false
	}
	if old != nil && old.row.SessionID == id {
		if old.row.Product != hello.Product || !slices.Equal(old.declaredGroups, hello.Groups) {
			return nil, nil, nil, false
		}
		old.row.Name, old.info = name, maps.Clone(hello.Info)
		return old, nil, nil, true
	}
	if found := d.entries[id]; found != nil && !found.peer {
		return nil, nil, nil, false
	}
	item := &entry{lifetime: &ownership{id: id, token: randomID("owner"), destinations: map[string]bool{}}, peer: true, declaredGroups: append([]string(nil), hello.Groups...), attachment: owner, done: make(chan struct{})}
	item.row = row{SessionID: id, Product: hello.Product, Name: name}
	item.row.Groups = orderedPeerGroups(hello.Groups, privateGroup(item))
	item.info = maps.Clone(hello.Info)
	if old != nil {
		delete(d.entries, old.row.SessionID)
		d.end(old)
		ended = old
	}
	if found := d.entries[id]; found != nil {
		displaced = found.attachment
		item.lifetime = found.lifetime
		found.lifetime = nil
		d.end(found)
	}
	d.entries[id] = item
	return item, displaced, ended, true
}

func (d *directory) current(item *entry, owner *session) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return item != nil && d.entries[item.row.SessionID] == item && item.attachment == owner
}

func (d *directory) finishRun(item *entry, owner *session) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item != nil && item.attachment == owner {
		item.running = false
	}
}

func (d *directory) admit(item *entry, owner *session, method string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item.attachment != owner {
		return protocol.NotConnected
	}
	if item.claimed && method != "message.deliver" {
		return protocol.Busy
	}
	if method == "turn.execute" && item.running {
		return protocol.Busy
	}
	if method == "turn.interrupt" && !item.running {
		return protocol.NotRunning
	}
	return 0
}

func (d *directory) admitted(item *entry, method string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if method == "turn.execute" {
		item.running = true
	}
	if method == "session.close" {
		item.claimed = true
	}
}

func (d *directory) detach(item *entry, owner *session) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item == nil || item.attachment != owner {
		return
	}
	d.end(item)
	if item.peer {
		delete(d.entries, item.row.SessionID)
	} else {
		item.claimed = true
	}
}

func (d *directory) offline(item *entry, owner *session, forget bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item.attachment == owner {
		item.attachment, item.running = nil, false
	}
	item.claimed = false
	if forget && d.entries[item.row.SessionID] == item {
		delete(d.entries, item.row.SessionID)
		delete(d.names, item.row.Name)
	}
}

func (d *directory) reserveFresh(value row, start *launch) (*entry, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if start.parent == nil || start.parent.ended {
		return nil, protocol.NotConnected
	}
	if d.names[value.Name] != nil {
		return nil, protocol.NameTaken
	}
	if code := d.addLaunch(start); code != 0 {
		return nil, code
	}
	item := &entry{row: cloneRow(value), parent: start.parent, claimed: true, done: make(chan struct{})}
	start.entry = item
	d.names[value.Name] = item
	return item, 0
}

func (d *directory) reserveResume(id string, groups []string, start *launch) (*entry, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if start.parent == nil || start.parent.ended {
		return nil, protocol.NotConnected
	}
	previous := d.entries[id]
	if previous == nil || previous.peer || !shares(groups, previous.row.Groups) {
		return nil, protocol.UnknownSession
	}
	if previous.claimed {
		return nil, protocol.Busy
	}
	if previous.attachment != nil {
		return nil, protocol.AlreadyConnected
	}
	policy, err := normalizePolicy(start.input, previous.row.Policy, start.parent.id)
	if err != nil {
		return nil, protocol.UnsupportedOpen
	}
	if code := d.addLaunch(start); code != 0 {
		return nil, code
	}
	item := &entry{row: cloneRow(previous.row), parent: start.parent, claimed: true, done: make(chan struct{})}
	item.row.Policy = policy
	start.previous = previous
	d.entries[id], d.names[item.row.Name], start.entry, start.product = item, item, item, item.row.Product
	return item, 0
}

func (d *directory) reserveDescribe(start *launch) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.addLaunch(start)
}

func (d *directory) addLaunch(start *launch) int {
	if d.closing {
		return protocol.Internal
	}
	d.tokens[start.token] = start
	d.daemon.group.Add(1)
	return 0
}

func (d *directory) claimWorker(owner *session, token, product string) (*launch, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	start := d.tokens[token]
	if start == nil || start.product != product || start.owner != nil {
		return nil, false
	}
	delete(d.tokens, token)
	start.owner = owner
	return start, true
}

func (d *directory) reserveID(item *entry, id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !item.claimed || d.entries[id] != nil {
		return false
	}
	item.row.SessionID = id
	d.entries[id] = item
	return true
}

func (d *directory) publish(start *launch, owner *session, createdAt time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	item := start.entry
	if d.closing || d.entries[item.row.SessionID] != item || !item.claimed || item.attachment != nil || item.parent != nil && item.parent.ended && !item.row.Policy.Persistent {
		return false
	}
	item.claimed = false
	item.attachment = owner
	item.lifetime = &ownership{id: item.row.SessionID, token: randomID("owner"), destinations: map[string]bool{}}
	if !createdAt.IsZero() {
		item.row.CreatedAt = createdAt
	}
	return true
}

func (d *directory) revokeLaunch(start *launch) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if start.owner != nil || d.tokens[start.token] != start {
		return false
	}
	delete(d.tokens, start.token)
	return true
}

func (d *directory) releaseLaunch(start *launch) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tokens[start.token] == start {
		delete(d.tokens, start.token)
	}
	item := start.entry
	if item == nil || !item.claimed {
		return
	}
	item.claimed = false
	d.end(item)
	if start.previous != nil {
		d.entries[item.row.SessionID] = start.previous
		d.names[item.row.Name] = start.previous
	}
	if !item.peer && item.row.CreatedAt.IsZero() {
		delete(d.names, item.row.Name)
		if d.entries[item.row.SessionID] == item {
			delete(d.entries, item.row.SessionID)
		}
	}
}

func (d *directory) resolveLocked(canonical string, groups []string) (*entry, bool) {
	if item := d.entries[canonical]; visibleTo(item, groups) {
		return item, false
	}
	var found *entry
	for _, item := range d.entries {
		if item.row.Name != "" && item.row.Name == canonical && visibleTo(item, groups) {
			if found != nil {
				return nil, true
			}
			found = item
		}
	}
	return found, false
}

func visibleTo(item *entry, groups []string) bool {
	return item != nil && (item.peer || !item.row.CreatedAt.IsZero()) && shares(groups, item.row.Groups)
}

func (d *directory) selectEntries(groups, labels []string, group string, names bool, omit *entry, request *routedRequest) ([]selected, int) {
	d.mu.Lock()
	result := make([]selected, 0, len(d.entries))
	if labels == nil {
		for _, item := range d.entries {
			if !visibleTo(item, groups) || group != "" && (item == omit || !slices.Contains(item.row.Groups, group)) {
				continue
			}
			result = append(result, selected{label: item.row.SessionID, item: item, summary: summarize(item)})
		}
		d.mu.Unlock()
		sort.Slice(result, func(left, right int) bool { return result[left].summary.SessionID < result[right].summary.SessionID })
		return result, 0
	}
	valid := validIDPart
	if names {
		valid = validNamePart
	}
	for _, label := range labels {
		canonical, code := canonical(label, d.daemon.host, valid)
		if code == protocol.InvalidFrame {
			d.mu.Unlock()
			return nil, code
		}
		item, ambiguous := d.resolveLocked(canonical, groups)
		value := selected{label: label, item: item, code: code, ambiguous: ambiguous}
		if item != nil {
			value.summary = summarize(item)
		}
		result = append(result, value)
	}
	if request != nil && result[0].code == 0 && result[0].item != nil && !result[0].ambiguous {
		result[0].code = d.routeLocked(result[0].item, request.method, *request)
	}
	d.mu.Unlock()
	return result, 0
}

func summarize(item *entry) protocol.SessionSummary {
	kind := "lane"
	if item.peer {
		kind = "peer"
	}
	return protocol.SessionSummary{SessionID: item.row.SessionID, Kind: kind, Product: item.row.Product, Name: item.row.Name,
		Groups: append([]string(nil), item.row.Groups...), Connected: item.attachment != nil, Running: item.running, Info: maps.Clone(item.info), Policy: cloneRow(item.row).Policy}
}

func (d *directory) route(item *entry, method string, request routedRequest) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.routeLocked(item, method, request)
}

func (d *directory) routeLocked(item *entry, method string, request routedRequest) int {
	if item == nil || d.entries[item.row.SessionID] != item {
		return protocol.NotConnected
	}
	if item.claimed && method != "message.deliver" {
		return protocol.Busy
	}
	if item.attachment == nil {
		return protocol.NotConnected
	}
	if item.peer && method != "message.deliver" {
		return protocol.UnknownSession
	}
	request.destination = item
	if item.attachment.wire.Post(request) {
		return 0
	}
	select {
	case <-item.attachment.wire.Done():
		return protocol.NotConnected
	default:
		return protocol.Busy
	}
}

func (d *directory) end(item *entry) {
	d.endOwner(item.lifetime)
	item.attachment, item.running = nil, false
	close(item.done)
}

func closedChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
