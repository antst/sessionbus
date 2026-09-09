// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"strings"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func ownerKey(id, token, attachment string) string { return id + "\x00" + token + "\x00" + attachment }
func (d *directory) remoteOwnerLocked(caller federation.Caller) *ownership {
	if caller.OwnerLifetime == "" || caller.SourceAttachment == "" {
		return nil
	}
	key := ownerKey(caller.SessionID, caller.OwnerLifetime, caller.SourceAttachment)
	if owner := d.remoteOwners[key]; owner != nil {
		return owner
	}
	host := caller.SessionID[strings.LastIndexByte(caller.SessionID, '@')+1:]
	owner := &ownership{id: caller.SessionID, token: caller.OwnerLifetime, host: host, attachment: caller.SourceAttachment}
	d.remoteOwners[key] = owner
	return owner
}
func (d *directory) forwardOwnerEnd(owner *ownership) {
	if owner.host != "" || d.daemon.federation == nil {
		return
	}
	for host := range owner.destinations {
		event := federation.OwnerEndCall{Value: federation.OwnerEnd{Host: host, SessionID: owner.id, Lifetime: owner.token}, Reply: make(chan federation.Reply, 1)}
		select {
		case d.daemon.federation.inbox <- event:
		default:
			d.daemon.federation.cancel()
		}
	}
}
func (d *directory) remoteEnded(event federation.LifetimeEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, owner := range d.remoteOwners {
		matches := event.Owner == nil && event.Host == nil
		if value := event.Owner; value != nil {
			matches = owner.id == value.SessionID && owner.token == value.Lifetime && owner.attachment == value.Attachment
		}
		if value := event.Host; value != nil {
			matches = owner.host == value.Host && owner.attachment == value.Attachment
		}
		if matches {
			d.endOwner(owner)
			delete(d.remoteOwners, key)
		}
	}
}
